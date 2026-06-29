// ============================================================================
//  GROUNDED — Firebase backend module (V27 / v27.0, May 2026)
//  ----------------------------------------------------------------------------
//  Pure ES module. Load it from grounded.html as:
//
//     <script type="module" src="firebase.js"></script>
//
//  It exposes three globals once Firebase has initialized:
//
//     window.groundedAuth   — authentication helpers
//     window.groundedSync   — Firestore + localStorage data sync
//     window.groundedReady  — Promise<{ groundedAuth, groundedSync }>
//                              resolves when first auth state is determined.
//                              The React app should `await` it before render.
//
//  Anonymous mode is fully supported: when no user is signed in, save/load
//  work against localStorage (key 'grounded_v1') exactly like V14. On first
//  sign-in any anonymous data is migrated to Firestore (server wins on
//  overlap), then localStorage is cleared.
//
//  Designed for the Firebase Spark (free) tier. No Cloud Functions required.
// ============================================================================


// ─── 1. Firebase config ─────────────────────────────────────────────────────
//
//  PASTE YOUR PROJECT VALUES HERE. Get them from:
//   Firebase Console → ⚙ Project Settings → "Your apps" → web app → SDK setup.
//  Once pasted, do NOT commit a public-facing apiKey to a public repo if you
//  intend to restrict it. Firebase Web apiKeys are *meant* to be public, BUT
//  you should still go to Google Cloud Console → Credentials → restrict the
//  key to your authorized domains. The setup guide will walk you through this.
//
const firebaseConfig = {
  apiKey: "AIzaSyBj-ogg028M7tGZkHNNvyf2LfjJq_x1a8k",
  authDomain: "grounded-firebase-v1.firebaseapp.com",
  projectId: "grounded-firebase-v1",
  storageBucket: "grounded-firebase-v1.firebasestorage.app",
  messagingSenderId: "684469836725",
  appId: "1:684469836725:web:11fe438778a6334d3597fc",
};

// ── Cloudflare Worker URL (V25 — Phase 2 backend) ──────────────────────────
//
//  Set after deploying the Worker (see Worker Setup Guide). Used by
//  groundedSync.startCheckout(), .openCustomerPortal(), .getFounderCount()
//  and (V25.4) .redeemCode(). When this is the placeholder string, all
//  payment helpers no-op gracefully — V20-style fallback behavior.
//
const WORKER_URL = "https://grounded-worker.grounded-api.workers.dev";

function workerConfigured() {
  return !!WORKER_URL && !WORKER_URL.startsWith("PASTE_");
}


// ─── 2. SDK imports (Firebase v10 modular, no build step) ───────────────────
//
import { initializeApp }
  from "https://www.gstatic.com/firebasejs/10.13.0/firebase-app.js";

import {
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  signInWithCustomToken,
  createUserWithEmailAndPassword,
  signInWithPopup,
  signOut as fbSignOut,
  sendPasswordResetEmail,
  sendEmailVerification as fbSendEmailVerification,
  updateProfile,
  updatePassword,
  verifyBeforeUpdateEmail,
  EmailAuthProvider,
  GoogleAuthProvider,
  reauthenticateWithCredential,
  reauthenticateWithPopup,
  deleteUser,
  linkWithPopup,
  linkWithCredential,
  setPersistence,
  browserLocalPersistence,
} from "https://www.gstatic.com/firebasejs/10.13.0/firebase-auth.js";

import {
  initializeFirestore,
  persistentLocalCache,
  persistentMultipleTabManager,
  doc,
  getDoc,
  setDoc,
  updateDoc,
  deleteDoc,
  deleteField,
  serverTimestamp,
  Timestamp,
  onSnapshot,
  runTransaction,
  // V57.7 — admin grant tool + audit log
  collection,
  addDoc,
  query,
  where,
  orderBy,
  limit as fsLimit,
  getDocs,
} from "https://www.gstatic.com/firebasejs/10.13.0/firebase-firestore.js";


// ─── 3. Initialize ──────────────────────────────────────────────────────────
//
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

// Persist auth across browser sessions (cookie-equivalent in IndexedDB).
setPersistence(auth, browserLocalPersistence).catch(() => { });

// Firestore with offline persistence + multi-tab sync.
const db = initializeFirestore(app, {
  localCache: persistentLocalCache({
    tabManager: persistentMultipleTabManager(),
  }),
});

const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({ prompt: "select_account" });


// ─── 4. Module state ────────────────────────────────────────────────────────
//
const LS_KEY = "grounded_v1";          // matches V14 — back-compat
const SAVE_DEBOUNCE_MS = 1500;
const USERNAME_LOCK_DAYS = 14;
const USERNAME_REGEX = /^[a-z0-9_.]+$/;
const USERNAME_MIN = 3;
const USERNAME_MAX = 24;

let currentUser = null;        // Firebase User or null
let currentDocData = {};          // last-known doc body (or LS data when anon)
let docUnsub = null;        // onSnapshot teardown
let saveTimer = null;        // pending debounce timer id
let pendingSave = null;        // pending merge-patch
let status = "loading";   // loading | syncing | idle | offline | anon

const authListeners = new Set();
const dataListeners = new Set();
const statusListeners = new Set();


// ─── 5. Helpers ─────────────────────────────────────────────────────────────
//
function setStatus(next) {
  if (status === next) return;
  status = next;
  statusListeners.forEach(cb => { try { cb(next); } catch { } });
}

function notifyData(data) {
  dataListeners.forEach(cb => { try { cb(data); } catch { } });
}

function notifyAuth(user) {
  authListeners.forEach(cb => { try { cb(user); } catch { } });
}

function loadAnonData() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch { return {}; }
}

function saveAnonData(data) {
  try { localStorage.setItem(LS_KEY, JSON.stringify(data)); } catch { }
}

function clearAnonData() {
  try { localStorage.removeItem(LS_KEY); } catch { }
}

function isOnline() {
  return typeof navigator !== "undefined" ? navigator.onLine : true;
}

// Firestore rejects undefined values. Strip them before any write.
function clean(value) {
  if (Array.isArray(value)) {
    return value.map(clean).filter(v => v !== undefined);
  }
  if (value && typeof value === "object" && !(value instanceof Date)) {
    // Skip Firestore sentinels (serverTimestamp etc.) — they're objects with
    // a private _methodName and aren't meant to be walked.
    if (value._methodName) return value;
    const out = {};
    for (const k of Object.keys(value)) {
      const v = clean(value[k]);
      if (v !== undefined) out[k] = v;
    }
    return out;
  }
  return value;
}

// Normalize a username for the uniqueness key (case-insensitive lookup).
function usernameKey(name) {
  return String(name || "").trim().toLowerCase();
}

// Online/offline awareness (only meaningful while signed-in).
window.addEventListener("online", () => { if (currentUser) setStatus("idle"); });
window.addEventListener("offline", () => { if (currentUser) setStatus("offline"); });


// ─── 5b. Subscription internals (V24 — Phase 2 read-path scaffolding) ───────
//
//  V24 adds a `subscription` object on every user doc. This is read-only
//  scaffolding — no Stripe / webhook / Worker logic yet. See PUBLIC API
//  §10 for the helpers Frontend uses to read this state.
//
//  Defensive defaults during V24/V25 dev period:
//    A1: doc has no subscription field → effectiveTier = 'pro' (full access)
//        Lets you and Frontend test the integration without needing every
//        test doc to have a fully-formed subscription object set up first.
//    B1: status='trial' but trialEndDate < now → effectiveTier = null
//        Soft trial expiry — webhook (V26) will hard-flip status to 'lapsed';
//        until then this fakes it client-side so demos feel real.
//
//  The eventual V27 migration script ensures every existing user doc has
//  a proper subscription object, retiring A1's defensive fallback.

const TRIAL_DAYS = 7;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** The subscription object every newly-created user starts with. */
function defaultTrialSubscription() {
  return {
    status: "trial",
    tier: null,
    lookupKey: null,              // v26.0 — Stripe Price lookup_key (premium_founder etc.)
    lockedRate: null,
    isFounder: false,
    founderNumber: null,
    founderStatusForfeited: false,// v26.0 — set true on downgrade away from founder tier
    isLifetime: false,
    comp: null,
    stripeCustomerId: null,
    stripeSubscriptionId: null,
    pricingPlanId: null,
    trialStartDate: serverTimestamp(),
    trialEndDate: Timestamp.fromDate(new Date(Date.now() + TRIAL_DAYS * MS_PER_DAY)),
    paidStartAt: null,            // set by webhook on first invoice.payment_succeeded
    currentPeriodEnd: null,
    cancelAtPeriodEnd: false,
    cancelledAt: null,            // v26.0 — set by webhook on customer.subscription.deleted
    lastPaymentAt: null,          // v26.0 — set by webhook on invoice.payment_succeeded
    lastPaymentFailedAt: null,    // v26.0 — set by webhook on invoice.payment_failed
    gracePeriodEndDate: null,
    lastWebhookEventId: null,     // webhook idempotency key — last Stripe event ID applied
    pricingHistory: [],
    // v27.0 — Account deletion cool-off
    scheduledForDeletion: null,   // timestamp — set by Worker /requestAccountDeletion
    deletionRequestedAt: null,    // timestamp — when user clicked delete
    // v27.0 — mirrors auth.token.email_verified for FE state access.
    // Backend syncs this via Layer C when JWT claim changes (e.g., after user
    // clicks verification link). FE reads from subscription.emailVerified.
    emailVerified: false,
  };
}

/** Pure read on a Timestamp-or-null field. Returns ms or null. */
function tsMillis(t) {
  return (t && typeof t.toMillis === "function") ? t.toMillis() : null;
}

/**
 * V55 / P0 trial-trap root-cause fix (May 25, 2026).
 *
 * Normalize subscription status strings at the read boundary. The Worker
 * (worker/src/index.js L1491, L1529) writes the British spelling 'cancelled'
 * to Firestore even though Stripe's API uses American 'canceled'. Pre-V55
 * the FE only matched American, so every cancelled-status user fell through
 * to the "unknown status" path → TRIAL fallback render → gate trap. Twenty-
 * four versions of patches missed this because British and American differ
 * by a single 'l' and look identical to a fast code review.
 *
 * This helper canonicalizes BOTH spellings (and any future casing/whitespace
 * weirdness) to a single source of truth. Use it anywhere we compare
 * subscription.status to a literal string. See full investigation at:
 *   Version 19 - Pattern 3B Deferred Signup/BUG_TRIAL_TRAP_INVESTIGATION.md
 */
function normalizeSubStatus(raw) {
  if (typeof raw !== "string") return null;
  const s = raw.trim().toLowerCase();
  // Cancelled-variants: Stripe writes 'canceled' (en-US), Worker rewrites it
  // to 'cancelled' (en-GB) before Firestore write. Both collapse to 'canceled'.
  if (s === "canceled" || s === "cancelled") return "canceled";
  return s;
}

/**
 * The single source of truth for tier resolution. Frontend MUST go through
 * groundedSync.getEffectiveTier() — never read raw subscription fields.
 *
 * Truth table (first match wins):
 *   no sub field            → 'pro'      (A1 defensive default for V24 dev)
 *   isLifetime              → 'premium'  (5 named testers, never charged)
 *   comp 'grace14_v20'      → 'premium' if grace not expired, else null
 *   status 'trial'          → 'pro' if trial not expired (B1), else null
 *   status 'active'/'grace' → tier ('premium'|'pro') or null if missing
 *   anything else (lapsed)  → null
 *
 * V55: status is normalized via normalizeSubStatus() to collapse the British
 * 'cancelled' / American 'canceled' duplicate. Without this, cancelled-status
 * users got `null` here AND fell into the TRIAL fallback UI in the frontend.
 */
function computeEffectiveTier(sub) {
  if (!sub) return "pro";                                          // A1
  if (sub.isLifetime) return "premium";

  if (sub.comp === "grace14_v20") {
    const ends = tsMillis(sub.gracePeriodEndDate);
    return (ends !== null && ends > Date.now()) ? "premium" : null;
  }

  // V55: read via normalizer — both 'canceled' and 'cancelled' become 'canceled'
  const status = normalizeSubStatus(sub.status);

  if (status === "trial") {
    const ends = tsMillis(sub.trialEndDate);
    if (ends !== null && ends < Date.now()) return null;            // B1
    return "pro";
  }

  if (status === "active" || status === "grace") {
    return (sub.tier === "premium" || sub.tier === "pro") ? sub.tier : null;
  }

  return null;
}


// ─── 6. Migration (anonymous localStorage → Firestore on first sign-in) ─────
//
//  Idempotent — safe to call repeatedly. Three cases:
//
//    A) localStorage has data, no remote doc
//        → upload localStorage as initial doc, mark migratedFromLocalStorage,
//          then clear localStorage.
//
//    B) localStorage has data, remote doc exists
//        → merge: remote wins on overlapping keys; local-only fields (and
//          local-only dates inside date-keyed maps) are added.
//          Then clear localStorage.
//
//    C) No localStorage, no remote doc
//        → create empty doc with system fields.
//
//    D) No localStorage, remote doc exists
//        → no-op.
//
//  Server-wins-on-overlap is enforced for date-keyed maps too: for each map,
//  we union the keys but if the same date exists both places, the remote
//  entry is preserved. Stale local data never overwrites server data.
//
const DATE_KEYED_MAPS = [
  "mirrorHistory",
  "reflections",
  "freeWrites",
  "completed",
  "launchpadHistory",
  "groundMeHistory",
];

function mergeForMigration(local, remote) {
  // Top-level: remote-wins-on-overlap.
  const out = { ...local, ...remote };

  // Date-keyed maps: union keys, remote wins per date.
  for (const key of DATE_KEYED_MAPS) {
    const localMap = (local && local[key]) || {};
    const remoteMap = (remote && remote[key]) || {};
    if (Object.keys(localMap).length || Object.keys(remoteMap).length) {
      out[key] = { ...localMap, ...remoteMap };
    }
  }
  return out;
}

// ─── v26.1 — Data preservation hotfix ───────────────────────────────────────
//
// Bug discovered May 19, 2026: an existing onboarded user cleared their
// browser cache, signed back in, and had their Firestore doc REPLACED with
// a fresh trial-state payload. Root cause: Cases A and C used setDoc WITHOUT
// merge:true (full document REPLACE), and `getDoc` returned null erroneously
// from the stale IndexedDB cache, so Case A/C fired for an existing user.
//
// Three-layer defense added in v26.1:
//
//   LAYER C — Data preservation guard:
//     If remote.mirrorBaselineDate is set, the user is fully onboarded.
//     ABORT all writes. No light-touch update, no merge — pure no-op return.
//     This is the explicit guard Parn's dispatch requested.
//
//   LAYER B — merge:true on Cases A/C:
//     Defense-in-depth so that even if Layer C is bypassed, the writes
//     can't replace an existing document, only update specified fields.
//
//   LAYER D — Wrap in runTransaction:
//     tx.get() reads with strong server consistency (not cache), and tx.set()
//     commits atomically. Eliminates the cache-race that triggered the bug.
//     If state changes during the transaction, Firestore retries the callback.
//
async function ensureUserDocAndMigrate(user) {
  const userRef = doc(db, "users", user.uid);

  // Capture localStorage state outside the transaction (won't change during txn)
  const local = loadAnonData();
  const hasLocal = Object.keys(local).length > 0;

  // Auth-derived fields (computed once)
  const baseUser = {
    email:       user.email || "",
    displayName: user.displayName || "",
    photoURL:    user.photoURL || "",
    providers:   user.providerData.map(p => p.providerId),
  };

  let result;
  try {
    result = await runTransaction(db, async (tx) => {
      // tx.get() = strong server read (NOT cache). This is LAYER D —
      // eliminates the cache race that caused the original bug.
      const snap = await tx.get(userRef);
      const remote = snap.exists() ? snap.data() : null;

      // ── LAYER C — Data preservation guard (v26.1) + emailVerified sync (v27.0) ──
      // An existing user with mirrorBaselineDate has completed onboarding.
      // ABORT all destructive writes. ONE exception (v27.0): if the JWT
      // claim email_verified has changed (e.g., user just clicked the
      // verification link), sync subscription.emailVerified to the new value
      // so FE can see it without re-reading the token. This is a single
      // nested-field merge — does not touch any user data.
      if (remote && remote.mirrorBaselineDate) {
        const authEmailVerified = !!user.emailVerified;
        const storedEmailVerified = remote.subscription && remote.subscription.emailVerified === true;
        if (authEmailVerified !== storedEmailVerified) {
          tx.set(userRef, {
            subscription: { emailVerified: authEmailVerified },
            updatedAt: serverTimestamp(),
          }, { merge: true });
          console.log(
            '[grounded] LAYER C — onboarded user (uid=' + user.uid +
            '), emailVerified sync: ' + storedEmailVerified + ' → ' + authEmailVerified
          );
          return { migrated: false, source: "existing-onboarded-emailVerified-synced" };
        }
        console.warn(
          '[grounded] LAYER C — existing onboarded user (uid=' + user.uid +
          ') — aborting all writes to preserve data integrity'
        );
        return { migrated: false, source: "existing-onboarded-guarded" };
      }

      // ── Case C: brand-new user, nothing to migrate ────────────────────
      // LAYER B: merge:true added in v26.1 as defense-in-depth.
      if (!remote && !hasLocal) {
        tx.set(userRef, clean({
          schemaVersion: 1,
          createdAt: serverTimestamp(),
          updatedAt: serverTimestamp(),
          migratedFromLocalStorage: false,
          user: baseUser,
          subscription: defaultTrialSubscription(),
        }), { merge: true });
        return { migrated: false, source: "fresh" };
      }

      // ── Case A: upload localStorage as initial doc ────────────────────
      // LAYER B: merge:true added in v26.1 as defense-in-depth.
      if (!remote && hasLocal) {
        tx.set(userRef, clean({
          ...local,
          schemaVersion: 1,
          createdAt: serverTimestamp(),
          updatedAt: serverTimestamp(),
          migratedFromLocalStorage: true,
          user: { ...(local.user || {}), ...baseUser },
          subscription: defaultTrialSubscription(),
        }), { merge: true });
        return { migrated: true, source: "fresh-upload" };
      }

      // ── Case B: merge — remote wins on overlap, local fills gaps ──────
      if (remote && hasLocal) {
        const merged = mergeForMigration(local, remote);
        merged.migratedFromLocalStorage = true;
        merged.updatedAt = serverTimestamp();
        merged.user = { ...(merged.user || {}), ...baseUser };
        tx.set(userRef, clean(merged), { merge: true });
        return { migrated: true, source: "merge" };
      }

      // ── Case D: remote exists but mirrorBaselineDate not set ──────────
      // (Partially-onboarded user — signed up but hasn't finished Mirror.)
      // Light-touch profile update is safe here because Layer C already
      // diverted the truly-onboarded case above.
      tx.set(userRef, clean({
        user: { ...(remote.user || {}), ...baseUser },
        updatedAt: serverTimestamp(),
      }), { merge: true });
      return { migrated: false, source: "existing" };
    });
  } catch (e) {
    console.warn('[grounded] ensureUserDocAndMigrate transaction failed:', e);
    throw e;
  }

  // Post-transaction side effects (MUST be outside the transaction so they
  // run only on successful commit, not on retry iterations).
  if (result.source === "fresh-upload" || result.source === "merge") {
    clearAnonData();
  }

  return result;
}


// ─── 7. Auth-state listener (the heart of the module) ───────────────────────
//
let resolveReady;
const readyPromise = new Promise(r => { resolveReady = r; });

onAuthStateChanged(auth, async (user) => {
  // Tear down previous user's subscription + cancel pending writes
  if (docUnsub) { try { docUnsub(); } catch { } docUnsub = null; }
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; pendingSave = null; }

  currentUser = user;

  if (!user) {
    // Anonymous mode — pull from localStorage, no migration
    setStatus("anon");
    currentDocData = loadAnonData();
    notifyAuth(null);
    notifyData(currentDocData);
    if (resolveReady) { resolveReady({ groundedAuth, groundedSync }); resolveReady = null; }
    return;
  }

  // Signed in — ensure doc, run migration if needed
  setStatus("syncing");
  try {
    await ensureUserDocAndMigrate(user);
  } catch (e) {
    console.warn("[grounded] migration failed:", e);
  }

  // Subscribe to live updates (this is what enables multi-device sync)
  const userRef = doc(db, "users", user.uid);
  docUnsub = onSnapshot(
    userRef,
    (snap) => {
      currentDocData = snap.data() || {};
      // Only flip to idle if we're not in the middle of a debounced save
      if (!pendingSave) setStatus(isOnline() ? "idle" : "offline");
      notifyData(currentDocData);
    },
    (err) => {
      console.warn("[grounded] doc snapshot error:", err);
      setStatus("offline");
    }
  );

  notifyAuth(user);
  if (resolveReady) { resolveReady({ groundedAuth, groundedSync }); resolveReady = null; }
});


// ─── 8. Debounced save (signed-in writes route through here) ────────────────
//
function scheduleSave(patch) {
  pendingSave = pendingSave ? { ...pendingSave, ...patch } : { ...patch };
  if (saveTimer) clearTimeout(saveTimer);
  setStatus("syncing");
  saveTimer = setTimeout(flushPendingSave, SAVE_DEBOUNCE_MS);
}

async function flushPendingSave() {
  saveTimer = null;
  if (!pendingSave || !currentUser) {
    pendingSave = null;
    setStatus(currentUser ? "idle" : "anon");
    return;
  }
  const patch = pendingSave;
  pendingSave = null;

  try {
    const userRef = doc(db, "users", currentUser.uid);
    await setDoc(
      userRef,
      clean({ ...patch, updatedAt: serverTimestamp() }),
      { merge: true }
    );
    setStatus(isOnline() ? "idle" : "offline");
  } catch (e) {
    console.warn("[grounded] save failed:", e);
    setStatus(isOnline() ? "idle" : "offline");
  }
}

// Best-effort flush on page unload. The Firestore SDK queues writes to
// IndexedDB synchronously, so the data survives a tab close even if the
// HTTP request hasn't completed.
window.addEventListener("beforeunload", () => {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  if (pendingSave && currentUser) {
    const patch = pendingSave;
    pendingSave = null;
    try {
      const userRef = doc(db, "users", currentUser.uid);
      setDoc(userRef, clean({ ...patch, updatedAt: serverTimestamp() }), { merge: true })
        .catch(() => { });
    } catch { }
  }
});


// ─── 9. PUBLIC API: groundedAuth ────────────────────────────────────────────
//
const groundedAuth = {
  get currentUser() { return currentUser; },

  /** Subscribe to auth state. Called immediately with current user. */
  onAuthChange(cb) {
    authListeners.add(cb);
    queueMicrotask(() => { try { cb(currentUser); } catch { } });
    return () => authListeners.delete(cb);
  },

  // ── V51 / Pattern 3B — Worker-mediated deferred signup ───────────────────
  //
  //  The old createUserWithEmailAndPassword flow created a Firebase Auth user
  //  the moment the user clicked Create Account, before they verified their
  //  email. That left orphaned/typo'd emails permanently "taken" in Firebase
  //  Auth and let bots pollute the user list.
  //
  //  New flow (no Firebase user until email is verified):
  //
  //    1. SignUpScreen.handleSubmit → groundedAuth.requestSignup(email, pwd, name)
  //       → POST {WORKER_URL}/requestSignup. Worker stores a pending-signup
  //       row keyed by email + emails the user a verification link with a
  //       short-lived token embedded in the URL.
  //
  //    2. User clicks link → lands on the PWA at ?token=… → App's boot-time
  //       URL-param handler calls groundedAuth.completeSignup(token).
  //       → POST {WORKER_URL}/completeSignup. Worker uses Admin SDK to
  //       admin.auth().createUser({email, password, displayName,
  //       emailVerified: true}) and mints a Firebase custom auth token.
  //       FE then calls signInWithCustomToken — the existing onAuthChange
  //       listener catches it and hydration routes through Trial Welcome.
  //
  //    3. Resend / Cancel from VerifyEmailScreen → groundedAuth.resendSignupEmail
  //       / groundedAuth.cancelSignup — both pass {email} (FE never holds the
  //       token; only the email knows it).
  //
  //  Worker contract (lock'd with Backend room May 22, 2026):
  //    POST /requestSignup    {email, password, displayName?}  → {ok}
  //                                                           failures:
  //                                                             email-already-exists
  //                                                             weak-password
  //                                                             invalid-email
  //                                                             send-failed
  //    POST /completeSignup   {token}                          → {ok, customToken}
  //                                                           failures:
  //                                                             token-expired
  //                                                             token-used
  //                                                             token-invalid
  //    POST /resendSignup     {email}                          → {ok}
  //    POST /cancelSignup     {email}                          → {ok}

  async requestSignup(email, password, displayName) {
    if (!workerConfigured()) return { ok: false, code: "worker-not-configured" };
    try {
      const res = await fetch(`${WORKER_URL}/requestSignup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: email,
          password: password,
          displayName: displayName || "",
        }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      return { ok: true };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  async completeSignup(token) {
    if (!workerConfigured()) return { ok: false, code: "worker-not-configured" };
    if (!token || typeof token !== "string") {
      return { ok: false, code: "invalid-token" };
    }
    try {
      const res = await fetch(`${WORKER_URL}/completeSignup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      // V51 / Pattern 3B — Worker fallback path. The Firebase Auth user has
      // been created (admin.auth().createUser succeeded), but the custom
      // token mint failed. Worker returns ok:true so FE doesn't treat this
      // as account-creation failure — instead surfaces the email so the
      // deep-link handler can prefill the signin screen and let the user
      // enter their password manually. We do NOT attempt signInWithCustomToken
      // in this branch (no token to sign with).
      if (data.needsManualSignIn === true || !data.customToken) {
        return {
          ok: true,
          needsManualSignIn: true,
          email: data.email || null,
        };
      }
      // Happy path: signInWithCustomToken fires onAuthStateChanged; the
      // existing listener chain (firebase.js §7 + V51 HTML A3 + hydration
      // watcher) takes it from here. Worker is responsible for having set
      // emailVerified: true on the Auth user record via Admin SDK, so
      // user.emailVerified is true on the first onAuthChange tick —
      // Trial Welcome routing works unchanged.
      const cred = await signInWithCustomToken(auth, data.customToken);
      return { ok: true, user: cred.user };
    } catch (e) {
      return { ok: false, code: e.code || "sign-in-failed", error: e.message };
    }
  },

  async resendSignupEmail(email) {
    if (!workerConfigured()) return { ok: false, code: "worker-not-configured" };
    if (!email || typeof email !== "string") {
      return { ok: false, code: "invalid-email" };
    }
    try {
      const res = await fetch(`${WORKER_URL}/resendSignup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      return { ok: true };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  async cancelSignup(email) {
    if (!workerConfigured()) return { ok: false, code: "worker-not-configured" };
    if (!email || typeof email !== "string") {
      return { ok: false, code: "invalid-email" };
    }
    try {
      const res = await fetch(`${WORKER_URL}/cancelSignup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      return { ok: true };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  /**
   * @deprecated V51/Pattern 3B — no longer called by SignUpScreen.
   *   Use requestSignup + completeSignup instead. Kept for backwards compat
   *   only (no in-tree callers as of V51). Will be removed in a future cleanup.
   */
  async signUpWithEmail(email, password, displayName) {
    try {
      const cred = await createUserWithEmailAndPassword(auth, email, password);
      if (displayName) {
        await updateProfile(cred.user, { displayName });
      }
      return { ok: true, user: cred.user };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async signInWithEmail(email, password) {
    // V57.5 P0 #2 — Diagnostic logging for linked-account email/password
    // hangs (Parn's case: G+email linked → email login spins forever).
    // We log the elapsed ms so future reports include actionable timing data.
    // The wrapper catches Firebase auth errors as before; the only difference
    // is what we record in console for triage.
    const t0 = Date.now();
    try {
      const cred = await signInWithEmailAndPassword(auth, email, password);
      const elapsed = Date.now() - t0;
      if (elapsed > 3000) {
        console.warn('[V57.5/signInWithEmail] SLOW resolve — elapsed=' + elapsed + 'ms · this may indicate a linked-account state issue');
      }
      // Defensive validation: cred.user must exist + have uid. If Firebase
      // returns a hollow cred object we flag rather than report ok:true blindly.
      if (!cred || !cred.user || !cred.user.uid) {
        console.error('[V57.5/signInWithEmail] resolved but cred.user is incomplete:', cred);
        return { ok: false, code: 'auth-record-incomplete', error: 'Sign-in returned an incomplete user record.' };
      }
      return { ok: true, user: cred.user };
    } catch (e) {
      const elapsed = Date.now() - t0;
      console.warn('[V57.5/signInWithEmail] failed — code=' + (e && e.code) + ' · elapsed=' + elapsed + 'ms');
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async signInWithGoogle() {
    // V57.5 P0 #1 — Defensive logging for Pim-style silent failures.
    // User Pim attempted Google sign-in and saw the Account screen as if
    // success, but Firebase Auth had no record. The fix here can't repair
    // a Firebase server failure, but it can REFUSE to report ok:true when
    // cred.user is missing critical fields, so the UI surfaces an error
    // rather than rendering a logged-in shell over a nonexistent user.
    const t0 = Date.now();
    try {
      const cred = await signInWithPopup(auth, googleProvider);
      const elapsed = Date.now() - t0;
      if (elapsed > 5000) {
        console.warn('[V57.5/signInWithGoogle] SLOW resolve — elapsed=' + elapsed + 'ms');
      }
      // Defensive validation
      if (!cred || !cred.user) {
        console.error('[V57.5/signInWithGoogle] resolved without cred.user — likely popup quirk:', cred);
        return { ok: false, code: 'auth-popup-empty', error: 'Sign-in popup returned no user.' };
      }
      if (!cred.user.uid) {
        console.error('[V57.5/signInWithGoogle] cred.user.uid missing — Firebase Auth record likely failed to write:', cred.user);
        return { ok: false, code: 'auth-record-missing', error: 'Sign-in succeeded with Google but the account record was not created. Please try again.' };
      }
      if (!cred.user.email) {
        console.error('[V57.5/signInWithGoogle] cred.user.email missing — Google scope or consent issue:', cred.user.uid);
        // We still let this through (returned ok:true) because some Google
        // accounts genuinely omit the email scope, but we log loudly so it's
        // visible during launch-week triage.
        console.warn('[V57.5/signInWithGoogle] proceeding with email-less cred — may break Firestore writes that rely on user.email');
      }
      return { ok: true, user: cred.user };
    } catch (e) {
      const elapsed = Date.now() - t0;
      console.warn('[V57.5/signInWithGoogle] failed — code=' + (e && e.code) + ' · elapsed=' + elapsed + 'ms');
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async signOut() {
    try {
      // Flush any pending write BEFORE we lose the auth context
      if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; await flushPendingSave(); }
      await fbSignOut(auth);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async sendPasswordReset(email) {
    try {
      await sendPasswordResetEmail(auth, email);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /**
   * V27/C1 — Send the email verification link to the currently signed-in
   * user. Called by V50 SignUpScreen post-createUserWithEmailAndPassword
   * and by the Account Settings ResendVerifyButton. No-op for Google-only
   * users (currentUser.emailVerified is already true natively).
   */
  async sendEmailVerification() {
    if (!currentUser) return { ok: false, code: "no-user" };
    if (currentUser.emailVerified) return { ok: false, code: "already-verified" };
    try {
      await fbSendEmailVerification(currentUser);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /** Re-auths first (Firebase requirement for sensitive ops). */
  async changePassword(currentPwd, newPwd) {
    if (!currentUser) return { ok: false, code: "no-user" };
    if (!currentUser.email) return { ok: false, code: "no-email" };
    try {
      const cred = EmailAuthProvider.credential(currentUser.email, currentPwd);
      await reauthenticateWithCredential(currentUser, cred);
      await updatePassword(currentUser, newPwd);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /**
   * Sends a verification link to the new email. Email actually changes only
   * after the user clicks the link. currentPwd is required for password
   * users; ignored for Google-only users (we re-auth via popup instead).
   */
  async changeEmail(newEmail, currentPwd) {
    if (!currentUser) return { ok: false, code: "no-user" };
    try {
      const isPasswordUser = currentUser.providerData.some(p => p.providerId === "password");
      if (isPasswordUser) {
        const cred = EmailAuthProvider.credential(currentUser.email, currentPwd);
        await reauthenticateWithCredential(currentUser, cred);
      } else {
        await reauthenticateWithPopup(currentUser, googleProvider);
      }
      await verifyBeforeUpdateEmail(currentUser, newEmail);
      return { ok: true, message: "Verification link sent to " + newEmail };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async updateDisplayName(name) {
    if (!currentUser) return { ok: false, code: "no-user" };
    try {
      await updateProfile(currentUser, { displayName: name });
      // Mirror to Firestore so it's available on other devices
      scheduleSave({ user: { ...(currentDocData.user || {}), displayName: name } });
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /** Add Google as a sign-in method to an existing account. */
  async linkWithGoogle() {
    if (!currentUser) return { ok: false, code: "no-user" };
    try {
      await linkWithPopup(currentUser, googleProvider);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /** Add Email/Password to a Google-only account. */
  async linkPassword(password) {
    if (!currentUser) return { ok: false, code: "no-user" };
    if (!currentUser.email) return { ok: false, code: "no-email" };
    try {
      const cred = EmailAuthProvider.credential(currentUser.email, password);
      await linkWithCredential(currentUser, cred);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /**
   * Permanently delete the account: releases username, deletes user doc,
   * deletes auth user. Re-auth required.
   * For password users, pass currentPwd. For Google users, currentPwd is
   * ignored — a Google popup will trigger.
   */
  async deleteAccount(currentPwd) {
    if (!currentUser) return { ok: false, code: "no-user" };
    try {
      const isPasswordUser = currentUser.providerData.some(p => p.providerId === "password");
      if (isPasswordUser) {
        const cred = EmailAuthProvider.credential(currentUser.email, currentPwd);
        await reauthenticateWithCredential(currentUser, cred);
      } else {
        await reauthenticateWithPopup(currentUser, googleProvider);
      }

      const claimedName = currentDocData.user && currentDocData.user.username;
      const uid = currentUser.uid;

      // Free username (if any)
      if (claimedName) {
        try { await deleteDoc(doc(db, "usernames", usernameKey(claimedName))); } catch { }
      }
      // Delete user doc
      try { await deleteDoc(doc(db, "users", uid)); } catch { }
      // Finally delete the auth user (also signs them out)
      await deleteUser(currentUser);
      return { ok: true };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },
};


// ─── 10. PUBLIC API: groundedSync ───────────────────────────────────────────
//
const groundedSync = {
  /**
   * Subscribe to data changes. Fires immediately with current data, then
   * on every server snapshot or local save. Returns unsubscribe function.
   */
  subscribe(cb) {
    dataListeners.add(cb);
    queueMicrotask(() => { try { cb(currentDocData); } catch { } });
    return () => dataListeners.delete(cb);
  },

  /**
   * Subscribe to status changes:
   *   'loading'  — initializing
   *   'syncing'  — write in flight or pending
   *   'idle'     — caught up, online
   *   'offline'  — no network (writes queued by Firestore SDK)
   *   'anon'     — no user signed in (localStorage only)
   */
  subscribeStatus(cb) {
    statusListeners.add(cb);
    queueMicrotask(() => { try { cb(status); } catch { } });
    return () => statusListeners.delete(cb);
  },

  /** Synchronous read of last-known data. */
  getDataNow() {
    return currentDocData;
  },

  /**
   * Save a patch. Pass either a full data object (V14-style) or a partial
   * patch — Firestore merge-writes either way. Anonymous users → localStorage
   * immediately. Signed-in users → debounced 1500 ms.
   */
  save(patch) {
    if (!patch || typeof patch !== "object") return;

    if (currentUser) {
      // Optimistic local update so React reflects the change immediately
      currentDocData = { ...currentDocData, ...patch };
      notifyData(currentDocData);
      scheduleSave(patch);
    } else {
      // Anonymous — full overwrite to localStorage (matches V14 behavior)
      const merged = { ...loadAnonData(), ...patch };
      saveAnonData(merged);
      currentDocData = merged;
      notifyData(merged);
    }
  },

  /** Force any pending debounced write to fire now. Awaitable. */
  async flush() {
    if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
    await flushPendingSave();
  },

  // ── SUBSCRIPTION HELPERS (V24 — Phase 2 read-path scaffolding) ─────────────
  //
  //  Eight read-only helpers for tier-aware UI gating. Frontend MUST go
  //  through these — NEVER read raw subscription fields directly. Centralizing
  //  here prevents truth-table drift and field-name coupling across the app.
  //
  //  V24 = read-only. V25 adds Stripe Checkout, V26 adds webhook + founder
  //  counter, V27 adds the one-time grace migration. Helpers compose against
  //  the existing data-listener path so they're reactive automatically — no
  //  polling needed in Frontend.

  /**
   * Subscribe to subscription object changes. Fires immediately with current
   * value (or null if no sub field). Returns unsubscribe function.
   */
  subscribeSubscription(cb) {
    const wrap = (d) => { try { cb((d && d.subscription) || null); } catch { } };
    dataListeners.add(wrap);
    queueMicrotask(() => wrap(currentDocData));
    return () => dataListeners.delete(wrap);
  },

  /**
   * The single source of truth for tier-aware UI. Returns 'pro' | 'premium'
   * | null. Synchronous; reads from currentDocData.subscription.
   * Use this for ALL feature gating.
   */
  getEffectiveTier() {
    return computeEffectiveTier(currentDocData.subscription);
  },

  /**
   * App-level access gate. Returns true if the user should see the app at
   * all, false if they should be hard-paywalled.
   */
  canAccessApp() {
    return computeEffectiveTier(currentDocData.subscription) !== null;
  },

  /**
   * True if the user is currently in their 7-day trial AND the trial hasn't
   * expired yet. False after Day 7 even if status field still says 'trial'
   * (B1 soft-expiry — see §5b).
   */
  isInTrial() {
    const sub = currentDocData.subscription;
    if (!sub || sub.status !== "trial") return false;
    const ends = tsMillis(sub.trialEndDate);
    return ends === null || ends > Date.now();
  },

  /**
   * Days remaining in the 7-day trial. Returns int (can be negative if the
   * trial has expired but webhook hasn't transitioned status yet — V25/V26).
   * Returns null if the user is not in trial state.
   */
  getTrialDaysLeft() {
    const sub = currentDocData.subscription;
    if (!sub || sub.status !== "trial") return null;
    const ends = tsMillis(sub.trialEndDate);
    if (ends === null) return null;
    return Math.ceil((ends - Date.now()) / MS_PER_DAY);
  },

  /**
   * Days remaining in grace period (either V20 14-day welcome OR cancel-grace).
   * Returns int (can be negative briefly during transition windows).
   * Returns null if the user is not in any grace state.
   */
  getDaysLeftInGrace() {
    const sub = currentDocData.subscription;
    if (!sub) return null;
    const inGrace = sub.status === "grace" || sub.comp === "grace14_v20";
    if (!inGrace) return null;
    const ends = tsMillis(sub.gracePeriodEndDate);
    if (ends === null) return null;
    return Math.ceil((ends - Date.now()) / MS_PER_DAY);
  },

  /**
   * Visual badge type for the current user.
   *   'lifetime' — one of the 5 named comp testers
   *   'founder'  — among the first 200 paying users (set by V26 webhook)
   *   null       — neither
   * Lifetime takes precedence over founder if both are somehow true.
   */
  getBadgeType() {
    const sub = currentDocData.subscription;
    if (!sub) return null;
    if (sub.isLifetime) return "lifetime";
    if (sub.isFounder) return "founder";
    return null;
  },

  /**
   * Live count of founder slots claimed (out of 200).
   * V25.2: real implementation — fetches /founderCount from the Worker.
   * Returns null if the Worker isn't configured yet OR the request fails;
   * callers (notably getTierPricing) treat null as "founder available" so
   * the UI never blocks on an unreachable Worker.
   */
  async getFounderCount() {
    if (!workerConfigured()) return null;
    try {
      const res = await fetch(`${WORKER_URL}/founderCount`);
      if (!res.ok) return null;
      const data = await res.json();
      return (typeof data.count === "number") ? data.count : null;
    } catch {
      return null;
    }
  },

  /**
   * Founder-aware tier pricing. Reads founderCount and maps to THB prices.
   * Returns { premium, pro, founderAvailable }. Defensive default: when
   * count is unknown (Worker not deployed yet, or fetch failed), returns
   * founder pricing — V25 dev/test stays smooth before real Worker is live.
   *
   *   { premium: 99,  pro: 199, founderAvailable: true  }   — first 200 paying users
   *   { premium: 199, pro: 299, founderAvailable: false }   — post-founder pricing
   */
  async getTierPricing() {
    const count = await groundedSync.getFounderCount();
    const founderAvailable = (count === null) || (count < 200);
    return {
      premium:           founderAvailable ? 99  : 199,
      pro:               founderAvailable ? 199 : 299,
      founderAvailable,
    };
  },

  /**
   * Start a Stripe Checkout flow for the given tier. Redirects via
   * window.location.href on success. Returns { ok: false, code } on any
   * failure (no redirect happens then — caller can show an error toast).
   *
   * The Worker is the source-of-truth for the Price ID actually attached
   * to the Stripe sub. If founderCount races past 200 between getTierPricing
   * and this call, the Worker may use the standard Price ID and return that
   * in `pricing`. UI should reconcile after the success redirect.
   *
   *   tier:  'premium' | 'pro'
   *   opts:  { successUrl, cancelUrl }  — explicit absolute URLs (recommended)
   *          OR { returnPath: '/path' } — helper derives successUrl + cancelUrl
   *                                       by appending ?checkout=success / cancel
   *          OR omitted                — defaults to mindoday.com
   */
  async startCheckout(tier, opts) {
    if (tier !== "premium" && tier !== "pro") {
      return { ok: false, code: "invalid-tier" };
    }
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };

    // Resolve successUrl + cancelUrl from the various opts shapes
    let successUrl, cancelUrl;
    if (opts && typeof opts === "object") {
      successUrl = opts.successUrl;
      cancelUrl  = opts.cancelUrl;
      if (!successUrl || !cancelUrl) {
        const base = opts.returnPath || "https://mindoday.com";
        const sep  = base.includes("?") ? "&" : "?";
        successUrl = successUrl || `${base}${sep}checkout=success`;
        cancelUrl  = cancelUrl  || `${base}${sep}checkout=cancel`;
      }
    } else {
      const base = "https://mindoday.com";
      successUrl = `${base}?checkout=success`;
      cancelUrl  = `${base}?checkout=cancel`;
    }

    let token;
    try { token = await currentUser.getIdToken(); }
    catch (e) { return { ok: false, code: "token-failed", error: e.message }; }

    try {
      const res = await fetch(`${WORKER_URL}/createCheckoutSession`, {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ tier, successUrl, cancelUrl }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      // Optimistic flush of any pending debounced save before redirect — we're
      // about to leave the page; let local state land on Firestore first.
      try { await groundedSync.flush(); } catch {}
      // Redirect to Stripe Checkout
      window.location.href = data.url;
      return { ok: true, sessionId: data.sessionId, pricing: data.pricing };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  /**
   * v26.0 — Change a user's subscription plan (upgrade or downgrade).
   *
   *   newLookupKey: 'premium_standard' | 'pro_standard' | 'pro_founder' | 'premium_founder'
   *   opts: { acknowledgeDowngrade?: boolean }
   *
   *   For downgrades, you MUST pass acknowledgeDowngrade: true. The Worker
   *   returns 400 'downgrade_requires_acknowledgement' otherwise — Frontend
   *   should show the friction modal and then re-call with the flag set.
   *
   *   Returns:
   *     { ok: true, newTier, newLookupKey, effectiveAt: 'immediately' } on success
   *     { ok: false, code, error? } on any failure
   *
   *   The Worker calls Stripe to update the subscription with proration_behavior:
   *   'create_prorations' — user pays/refunds the prorated difference immediately.
   *   The webhook will then fire customer.subscription.updated and sync Firestore.
   */
  async changePlan(newLookupKey, opts = {}) {
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };

    const validKeys = ["premium_founder", "premium_standard", "pro_founder", "pro_standard"];
    if (!validKeys.includes(newLookupKey)) {
      return { ok: false, code: "invalid-lookup-key" };
    }

    let token;
    try { token = await currentUser.getIdToken(); }
    catch (e) { return { ok: false, code: "token-failed", error: e.message }; }

    try {
      const res = await fetch(`${WORKER_URL}/changePlan`, {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          newLookupKey,
          acknowledgeDowngrade: opts.acknowledgeDowngrade === true,
        }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return {
          ok: false,
          code: data.code || data.error || "http-error",
          error: data.message || data.error,
          currentTier: data.currentTier,
          newTier: data.newTier,
        };
      }
      // Optimistic flush of any pending debounced save before webhook fires
      try { await groundedSync.flush(); } catch {}
      return {
        ok: true,
        newTier: data.newTier,
        newLookupKey: data.newLookupKey,
        effectiveAt: data.effectiveAt,
      };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  /**
   * Open the Stripe Customer Portal so the user can manage their subscription
   * (update card, cancel, view invoices). Stripe-hosted UI. Redirects via
   * window.location.href on success.
   *
   *   opts:  { returnUrl: 'https://...' }  — absolute URL (recommended)
   *          OR { returnPath: '/path' }    — helper resolves to absolute
   *          OR omitted                    — defaults to mindoday.com
   *
   *   404 'no-subscription' means user has no Stripe customer record yet
   *   (they've never completed Checkout).
   */
  async openCustomerPortal(opts) {
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };

    let returnUrl;
    if (opts && typeof opts === "object") {
      returnUrl = opts.returnUrl || opts.returnPath || "https://mindoday.com";
    } else if (typeof opts === "string") {
      returnUrl = opts;
    } else {
      returnUrl = "https://mindoday.com";
    }

    let token;
    try { token = await currentUser.getIdToken(); }
    catch (e) { return { ok: false, code: "token-failed", error: e.message }; }

    try {
      const res = await fetch(`${WORKER_URL}/createPortalSession`, {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ returnUrl }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      try { await groundedSync.flush(); } catch {}
      window.location.href = data.url;
      return { ok: true };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  // ── v27.0 ACCOUNT LIFECYCLE HELPERS ──────────────────────────────────────
  //
  //  Soft delete with 30-day cool-off. Frontend flow:
  //    1. User confirms in Settings → FE calls requestAccountDeletion()
  //    2. Backend writes scheduledForDeletion + pauses Stripe sub
  //    3. User can sign back in during 30-day window — FE shows recovery
  //       modal — calls restoreAccount() to cancel deletion
  //    4. After 30 days, Worker cron force-deletes user + Auth user

  /**
   * Request account deletion (30-day cool-off period).
   *
   *   confirmText: must equal "DELETE MY ACCOUNT" (case-sensitive) — Frontend
   *                shows a confirmation field; user types it to confirm.
   *
   *   Returns:
   *     { ok: true, scheduledFor: <ISO>, daysUntilDeletion: 30 } on success
   *     { ok: false, code, error?, scheduledFor? } on failure
   *       code='already_scheduled' (409) — deletion already pending
   *       code='invalid_confirm_text' (400) — confirmText didn't match
   *       code='user-doc-not-found' (404) — no Firestore doc for user
   *
   *   NOTE: NOT gated on emailVerified — users can delete unverified accounts
   *   to free up their email.
   */
  async requestAccountDeletion(confirmText) {
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };
    if (confirmText !== "DELETE MY ACCOUNT") {
      return { ok: false, code: "invalid_confirm_text" };
    }

    let token;
    try { token = await currentUser.getIdToken(); }
    catch (e) { return { ok: false, code: "token-failed", error: e.message }; }

    try {
      const res = await fetch(`${WORKER_URL}/requestAccountDeletion`, {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ confirmText }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return {
          ok: false,
          code: data.code || data.error || "http-error",
          error: data.message || data.error,
          scheduledFor: data.scheduledFor,
        };
      }
      try { await groundedSync.flush(); } catch {}
      return {
        ok: true,
        scheduledFor: data.scheduledFor,
        daysUntilDeletion: data.daysUntilDeletion,
      };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  /**
   * Cancel a pending account deletion (within the 30-day window).
   * Resumes Stripe subscription if it was paused.
   *
   *   Returns:
   *     { ok: true, stripeResumed: bool } on success
   *     { ok: false, code, error? } on failure
   *       code='no_active_deletion' (404) — nothing pending to cancel
   *
   *   NOT gated on emailVerified — parallels requestAccountDeletion.
   */
  async restoreAccount() {
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };

    let token;
    try { token = await currentUser.getIdToken(); }
    catch (e) { return { ok: false, code: "token-failed", error: e.message }; }

    try {
      const res = await fetch(`${WORKER_URL}/restoreAccount`, {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
        body: "{}",
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        return {
          ok: false,
          code: data.code || data.error || "http-error",
          error: data.message || data.error,
        };
      }
      try { await groundedSync.flush(); } catch {}
      return { ok: true, stripeResumed: !!data.stripeResumed };
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  /**
   * Check whether an email address is scheduled for deletion.
   * PUBLIC endpoint — no auth required. Used by Frontend on duplicate-email
   * signup attempts to detect "cool-off" state and show recovery prompt.
   *
   *   Returns:
   *     { ok: true, scheduled: false } — email not registered OR not in cool-off
   *     { ok: true, scheduled: true, scheduledFor: <ISO>, daysRemaining: N }
   *     { ok: false, code, error? } on transport failure
   *
   *   Privacy: reveals only "email is registered + in cool-off." Same shape
   *   as Firebase Auth's natural email-already-in-use error.
   */
  async checkDeletionStatus(email) {
    if (!workerConfigured()) return { ok: false, code: "worker-not-configured" };
    if (!email || typeof email !== "string") {
      return { ok: false, code: "invalid-email" };
    }
    try {
      const url = `${WORKER_URL}/deletionStatus?email=${encodeURIComponent(email)}`;
      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok) {
        return { ok: false, code: data.code || "http-error", error: data.error };
      }
      return data;   // { ok, scheduled, scheduledFor?, daysRemaining? }
    } catch (e) {
      return { ok: false, code: "fetch-failed", error: e.message };
    }
  },

  // ── DELETE HELPERS (Bug 15 / V19) ─────────────────────────────────────────
  //
  //  Why these exist: setDoc(merge:true) treats missing keys as "don't update"
  //  not "delete." So `delete data.reflections[today]` on the React side
  //  followed by save() leaves the field intact on the server. These helpers
  //  use Firestore's deleteField() sentinel via updateDoc with dotted field
  //  paths to actually remove nested keys.
  //
  //  Each helper:
  //    1. Flushes any pending debounced save FIRST. Otherwise a stale patch
  //       could resurrect the just-deleted field via merge.
  //    2. Updates currentDocData optimistically + notifies subscribers, so
  //       React reflects the delete instantly.
  //    3. Issues the Firestore deleteField call (signed-in) or mutates
  //       localStorage (anonymous).
  //    4. Returns Promise<{ok, code?, error?}> matching the rest of the API.
  //
  //  Day-level cleanup of `completed[dayKey]` when its last exerciseId is
  //  removed is intentionally left to the Frontend's presentation layer —
  //  doing it server-side cleanly needs a read-then-write transaction.

  /** Delete a single reflection entry by date key (e.g. '2026-05-08'). */
  async deleteReflection(dayKey) {
    if (!dayKey || typeof dayKey !== "string") {
      return { ok: false, code: "invalid-key" };
    }
    if (currentUser) {
      // Land any pending debounced save first to avoid stale-patch resurrection
      if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
      await flushPendingSave();
      // Optimistic local update
      if (currentDocData.reflections && (dayKey in currentDocData.reflections)) {
        const next = { ...currentDocData.reflections };
        delete next[dayKey];
        currentDocData = { ...currentDocData, reflections: next };
        notifyData(currentDocData);
      }
      try {
        await updateDoc(doc(db, "users", currentUser.uid), {
          ["reflections." + dayKey]: deleteField(),
          updatedAt: serverTimestamp(),
        });
        return { ok: true };
      } catch (e) {
        return { ok: false, code: e.code, error: e.message };
      }
    }
    // Anonymous mode — mutate localStorage and notify
    const local = loadAnonData();
    if (local.reflections && (dayKey in local.reflections)) {
      delete local.reflections[dayKey];
      saveAnonData(local);
    }
    currentDocData = local;
    notifyData(currentDocData);
    return { ok: true };
  },

  /** Delete a single free-write entry by date key. Same semantics as deleteReflection. */
  async deleteFreeWrite(dayKey) {
    if (!dayKey || typeof dayKey !== "string") {
      return { ok: false, code: "invalid-key" };
    }
    if (currentUser) {
      if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
      await flushPendingSave();
      if (currentDocData.freeWrites && (dayKey in currentDocData.freeWrites)) {
        const next = { ...currentDocData.freeWrites };
        delete next[dayKey];
        currentDocData = { ...currentDocData, freeWrites: next };
        notifyData(currentDocData);
      }
      try {
        await updateDoc(doc(db, "users", currentUser.uid), {
          ["freeWrites." + dayKey]: deleteField(),
          updatedAt: serverTimestamp(),
        });
        return { ok: true };
      } catch (e) {
        return { ok: false, code: e.code, error: e.message };
      }
    }
    const local = loadAnonData();
    if (local.freeWrites && (dayKey in local.freeWrites)) {
      delete local.freeWrites[dayKey];
      saveAnonData(local);
    }
    currentDocData = local;
    notifyData(currentDocData);
    return { ok: true };
  },

  /**
   * Delete a single exercise entry on a given day. Targets the 2-level nested
   * path `completed.<dayKey>.<exerciseId>`. Confirmed safe with V19's 5
   * snake_case exerciseIds (do_for_me, thank_me, small_wins, my_strengths,
   * self_date) — all dot-free, so the dotted field path is unambiguous.
   * Day-level map left in place even if empty after delete (presentation concern).
   */
  async deleteExerciseEntry(dayKey, exerciseId) {
    if (!dayKey      || typeof dayKey      !== "string" ||
        !exerciseId  || typeof exerciseId  !== "string") {
      return { ok: false, code: "invalid-key" };
    }
    if (currentUser) {
      if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
      await flushPendingSave();
      // Optimistic local update
      const completed = { ...(currentDocData.completed || {}) };
      const dayMap = completed[dayKey];
      if (dayMap && (exerciseId in dayMap)) {
        const nextDay = { ...dayMap };
        delete nextDay[exerciseId];
        completed[dayKey] = nextDay;
        currentDocData = { ...currentDocData, completed };
        notifyData(currentDocData);
      }
      try {
        await updateDoc(doc(db, "users", currentUser.uid), {
          ["completed." + dayKey + "." + exerciseId]: deleteField(),
          updatedAt: serverTimestamp(),
        });
        return { ok: true };
      } catch (e) {
        return { ok: false, code: e.code, error: e.message };
      }
    }
    const local = loadAnonData();
    if (local.completed && local.completed[dayKey] && (exerciseId in local.completed[dayKey])) {
      delete local.completed[dayKey][exerciseId];
      saveAnonData(local);
    }
    currentDocData = local;
    notifyData(currentDocData);
    return { ok: true };
  },

  /**
   * Check username availability.
   * Returns { ok, available, owned?, reason? }.
   *   reason: 'invalid' if format/length wrong.
   *   owned:  true if the current user already owns this name.
   */
  async checkUsernameAvailable(name) {
    const key = usernameKey(name);
    if (key.length < USERNAME_MIN || key.length > USERNAME_MAX || !USERNAME_REGEX.test(key)) {
      return { ok: true, available: false, reason: "invalid" };
    }
    try {
      const snap = await getDoc(doc(db, "usernames", key));
      if (!snap.exists()) return { ok: true, available: true };
      const owned = currentUser && snap.data().uid === currentUser.uid;
      return { ok: true, available: !!owned, owned: !!owned };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /**
   * Atomically claim a username. Releases the user's previous username (if
   * any) and updates the user doc — all in a single transaction so we can't
   * end up in a half-state.
   * Returns { ok, code?, daysLeft? }. code='locked' means the 14-day rule
   * is still in effect; daysLeft tells the UI how long to wait.
   */
  async claimUsername(name) {
    if (!currentUser) return { ok: false, code: "no-user" };
    const newKey = usernameKey(name);
    if (newKey.length < USERNAME_MIN || newKey.length > USERNAME_MAX || !USERNAME_REGEX.test(newKey)) {
      return { ok: false, code: "invalid-username" };
    }

    // Client-side 14-day lock check (server enforces too).
    const changedAtField = currentDocData.user && currentDocData.user.usernameChangedAt;
    if (changedAtField && typeof changedAtField.toMillis === "function") {
      const ageMs = Date.now() - changedAtField.toMillis();
      const limitMs = USERNAME_LOCK_DAYS * 24 * 60 * 60 * 1000;
      if (ageMs < limitMs) {
        const daysLeft = Math.ceil((limitMs - ageMs) / (24 * 60 * 60 * 1000));
        return { ok: false, code: "locked", daysLeft };
      }
    }

    const oldKey = usernameKey(currentDocData.user && currentDocData.user.username);
    const userRef = doc(db, "users", currentUser.uid);
    const newRef = doc(db, "usernames", newKey);
    const oldRef = oldKey ? doc(db, "usernames", oldKey) : null;

    try {
      await runTransaction(db, async (tx) => {
        const newSnap = await tx.get(newRef);
        if (newSnap.exists() && newSnap.data().uid !== currentUser.uid) {
          throw new Error("username-taken");
        }
        if (oldRef && oldKey !== newKey) tx.delete(oldRef);
        tx.set(newRef, { uid: currentUser.uid, claimedAt: serverTimestamp() });
        tx.set(userRef, {
          user: {
            ...(currentDocData.user || {}),
            username: name.trim(),
            usernameChangedAt: serverTimestamp(),
          },
          updatedAt: serverTimestamp(),
        }, { merge: true });
      });
      return { ok: true };
    } catch (e) {
      const code = e.message === "username-taken" ? "taken" : (e.code || "error");
      return { ok: false, code, error: e.message };
    }
  },

  /** Export everything as a JSON-serializable object. */
  async exportAll() {
    if (currentUser) {
      try {
        const snap = await getDoc(doc(db, "users", currentUser.uid));
        return { ok: true, data: snap.exists() ? snap.data() : {} };
      } catch (e) {
        return { ok: false, code: e.code, error: e.message };
      }
    }
    return { ok: true, data: loadAnonData() };
  },

  /** Manually run migration (rarely needed — auto-runs on sign-in). */
  async migrateLocalStorage() {
    if (!currentUser) return { ok: false, code: "no-user" };
    try {
      const r = await ensureUserDocAndMigrate(currentUser);
      return { ok: true, ...r };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  // ─── V57.7 — Admin Founder Grant Tool ─────────────────────────────────────
  //
  //  ⚠️ SECURITY MODEL — READ BEFORE EDITING:
  //
  //  The client-side `/admin` password gate is UX-only — it prevents random
  //  users from stumbling onto the form. The REAL security boundary is
  //  Firestore Security Rules: only admin UID (or a custom-claim-marked
  //  admin) may write to other users' subscription docs. See
  //  ADMIN_TOOL_GUIDE.md → "Firestore rules required" for the rule snippet.
  //
  //  Without those rules, ANY signed-in user who guesses the password can
  //  grant themselves Premium forever. Do not deploy V57.7 without first
  //  pushing the matching Firestore rules.
  //
  //  Why client-side (not Worker / Admin SDK)?
  //    - Worker scope was explicitly out-of-scope per V57.7 dispatch
  //    - 5 named launch-week users need Premium NOW; Worker turnaround = days
  //    - Firestore Rules + Parn-UID enforcement is a defensible interim
  //    - V58 should migrate this to a Worker endpoint with Admin SDK
  //
  //  Architecture:
  //    1. Look up user by email via Firestore query on users.user.email
  //       (Admin SDK would query Firebase Auth; we don't have that client-side)
  //    2. Write subscription patch to users/{uid}: status=active, tier, isFounder
  //    3. Append audit row to code_redemptions/{auto-id}
  //
  //  All three writes happen from Parn's signed-in session — the Firestore
  //  rules are what gate the cross-user write.

  /**
   * V57.7 — Find a user UID by their email via Firestore query.
   * Returns { ok: true, uid, displayName, email } on success,
   * { ok: false, code: 'not-found' | 'multiple' | 'query-failed' } otherwise.
   *
   * Note: this queries the `users` collection on `user.email` field —
   * it does NOT query Firebase Auth directly (client SDK cannot).
   * Users who signed up but never had a Firestore doc created will not
   * be findable via this method.
   */
  async adminFindUserByEmail(email) {
    if (!email || typeof email !== "string") {
      return { ok: false, code: "invalid-email" };
    }
    if (!currentUser) {
      return { ok: false, code: "admin-not-signed-in", error: "Sign in first — Firestore rules require admin UID." };
    }
    const normalized = email.trim().toLowerCase();
    try {
      const usersRef = collection(db, "users");
      const q = query(usersRef, where("user.email", "==", normalized), fsLimit(2));
      const snap = await getDocs(q);
      if (snap.empty) {
        // Fallback: try the top-level `email` field too (some early users have it there)
        const q2 = query(usersRef, where("email", "==", normalized), fsLimit(2));
        const snap2 = await getDocs(q2);
        if (snap2.empty) return { ok: false, code: "not-found" };
        if (snap2.size > 1) return { ok: false, code: "multiple" };
        const d = snap2.docs[0];
        const data = d.data() || {};
        return { ok: true, uid: d.id, email: (data.user && data.user.email) || data.email || normalized, displayName: (data.user && data.user.displayName) || "" };
      }
      if (snap.size > 1) return { ok: false, code: "multiple" };
      const d = snap.docs[0];
      const data = d.data() || {};
      return { ok: true, uid: d.id, email: (data.user && data.user.email) || normalized, displayName: (data.user && data.user.displayName) || "" };
    } catch (e) {
      console.error("[V57.7/adminFindUserByEmail] query threw:", e);
      return { ok: false, code: "query-failed", error: e.message };
    }
  },

  /**
   * V57.7 — Grant Founder access directly to a user's Firestore subscription doc.
   * Bypasses Stripe checkout entirely (for users who never reached Stripe).
   *
   * Writes:
   *   1) users/{uid}.subscription: { status:'active', tier, isFounder:true,
   *      founderCode, founderGrantedAt, founderGrantedBy:'admin', founderReason,
   *      trialEnd:null }
   *   2) code_redemptions/{auto-id}: full audit row (see AUDIT_LOG_SCHEMA.md)
   *
   * Returns { ok, redemptionId? } / { ok:false, code, error }.
   */
  async adminGrantFounder({ email, tier, founderCode, reason }) {
    if (!email || typeof email !== "string") return { ok: false, code: "invalid-email" };
    if (tier !== "premium" && tier !== "pro") return { ok: false, code: "invalid-tier" };
    if (!currentUser) {
      return { ok: false, code: "admin-not-signed-in", error: "Sign in first — Firestore rules require admin UID." };
    }

    const lookup = await this.adminFindUserByEmail(email);
    if (!lookup.ok) return lookup;
    const { uid, email: foundEmail, displayName } = lookup;

    const code = (founderCode || "").trim() || "ADMIN_GRANT";
    const cleanReason = (reason || "").trim() || null;
    const grantedByEmail = currentUser.email || null;

    try {
      // 1) Update the target user's subscription state.
      await updateDoc(doc(db, "users", uid), {
        "subscription.status":            "active",
        "subscription.tier":              tier,
        "subscription.isFounder":         true,
        "subscription.founderCode":       code,
        "subscription.founderGrantedAt":  serverTimestamp(),
        "subscription.founderGrantedBy":  "admin",
        "subscription.founderGrantedByEmail": grantedByEmail,
        "subscription.founderReason":     cleanReason,
        "subscription.trialEndDate":      null,
        "subscription.lookupKey":         tier === "premium" ? "premium_founder" : "pro_founder",
        "subscription.lockedRate":        tier === "premium" ? 169 : 89,
        updatedAt: serverTimestamp(),
      });

      // 2) Append to audit log. Auto-id doc.
      const redemptionRef = await addDoc(collection(db, "code_redemptions"), clean({
        userEmail:    foundEmail,
        userId:       uid,
        userDisplayName: displayName || null,
        code,
        codeType:     "admin_grant",
        tier,
        grantedAt:    serverTimestamp(),
        method:       "admin",
        grantedBy:    "admin",
        grantedByUid: currentUser.uid,
        grantedByEmail,
        reason:       cleanReason,
      }));

      console.log("[V57.7/adminGrantFounder] granted " + tier + " to " + foundEmail + " (uid=" + uid + ") · redemptionId=" + redemptionRef.id);
      return { ok: true, uid, email: foundEmail, displayName, redemptionId: redemptionRef.id };
    } catch (e) {
      console.error("[V57.7/adminGrantFounder] write failed:", e);
      // Firestore permission denied = Firestore Rules not configured properly.
      if (e && e.code === "permission-denied") {
        return { ok: false, code: "permission-denied", error: "Firestore Rules blocked the write. Verify the admin rules are deployed (see ADMIN_TOOL_GUIDE.md)." };
      }
      return { ok: false, code: e.code, error: e.message };
    }
  },

  /**
   * V58 — Self-service founder code redemption.
   *
   * User pastes a code → frontend calls this → backend validates against
   * Firestore `founder_codes` collection → if valid, grants tier + status
   * active to user's subscription.
   *
   * Bypasses Stripe entirely. No Customer created, no Checkout, no webhook.
   * Codes are pre-seeded by admin (Parn) in founder_codes collection.
   *
   * Schema (founder_codes/{code}):
   *   {
   *     code: "FOUNDER_KENNY",
   *     type: "founder" | "campaign" | "referral",
   *     tier: "premium" | "pro",
   *     trialDays: null,       // null = lifetime, number = trial days
   *     maxRedemptions: 1,
   *     currentRedemptions: 0,
   *     expiresAt: null,
   *     campaign: "founder_launch",
   *     redeemedBy: []
   *   }
   *
   * Returns:
   *   { ok: true, tier, founderCode, isLifetime, trialDays, message }
   *   { ok: false, code: "invalid-code"|"expired"|"exhausted"|"already-redeemed"|... }
   */
  async redeemFounderCode(codeInput) {
    if (!codeInput || typeof codeInput !== "string") {
      return { ok: false, code: "invalid-input" };
    }
    if (!currentUser) {
      return { ok: false, code: "not-signed-in", error: "Please sign in first" };
    }

    const normalizedCode = codeInput.trim().toUpperCase();

    try {
      const codeRef = doc(db, "founder_codes", normalizedCode);
      const codeSnap = await getDoc(codeRef);

      if (!codeSnap.exists()) {
        return { ok: false, code: "invalid-code", error: "Code not found" };
      }

      const codeData = codeSnap.data();
      const now = Date.now();

      // Check expiration
      if (codeData.expiresAt) {
        const expiresMs = (codeData.expiresAt.toMillis)
          ? codeData.expiresAt.toMillis()
          : new Date(codeData.expiresAt).getTime();
        if (expiresMs < now) {
          return { ok: false, code: "expired", error: "This code has expired" };
        }
      }

      // Check max redemptions
      const currentCount = codeData.currentRedemptions || 0;
      const maxAllowed = codeData.maxRedemptions || 1;
      if (currentCount >= maxAllowed) {
        return { ok: false, code: "exhausted", error: "This code has been fully redeemed" };
      }

      // Check if THIS user already redeemed it
      const redeemedBy = codeData.redeemedBy || [];
      const alreadyRedeemed = redeemedBy.some(r => r.uid === currentUser.uid);
      if (alreadyRedeemed) {
        return { ok: false, code: "already-redeemed", error: "You've already used this code" };
      }

      // Validate tier
      const tier = codeData.tier;
      if (tier !== "premium" && tier !== "pro") {
        return { ok: false, code: "invalid-tier-in-code" };
      }

      // Compute trial end (if not lifetime)
      let trialEndDate = null;
      if (codeData.trialDays && codeData.trialDays > 0) {
        trialEndDate = Timestamp.fromDate(new Date(now + codeData.trialDays * 24 * 60 * 60 * 1000));
      }
      const isLifetime = !codeData.trialDays;
      const lockedRate = tier === "pro" ? 199 : 99;
      const lookupKey = tier === "pro" ? "pro_founder" : "premium_founder";

      // Update user subscription
      const userRef = doc(db, "users", currentUser.uid);
      await updateDoc(userRef, {
        "subscription.status": "active",
        "subscription.tier": tier,
        "subscription.isFounder": codeData.type === "founder",
        "subscription.founderCode": normalizedCode,
        "subscription.founderGrantedAt": serverTimestamp(),
        "subscription.founderGrantedBy": "self-redemption",
        "subscription.founderGrantedByEmail": currentUser.email || null,
        "subscription.founderReason": "Self-redeemed via in-app code entry (" + (codeData.type || "founder") + ")",
        "subscription.trialEndDate": trialEndDate,
        "subscription.lockedRate": lockedRate,
        "subscription.lookupKey": lookupKey,
        "subscription.isLifetime": isLifetime,
        updatedAt: serverTimestamp(),
      });

      // Update the code document
      await updateDoc(codeRef, {
        currentRedemptions: currentCount + 1,
        redeemedBy: [
          ...redeemedBy,
          {
            uid: currentUser.uid,
            email: currentUser.email || null,
            redeemedAt: new Date().toISOString(),
          }
        ],
        lastRedeemedAt: serverTimestamp(),
      });

      // Append audit log row
      try {
        await addDoc(collection(db, "code_redemptions"), clean({
          userEmail:    currentUser.email || null,
          userId:       currentUser.uid,
          code:         normalizedCode,
          codeType:     codeData.type || "founder",
          tier,
          isLifetime,
          trialDays:    codeData.trialDays || null,
          grantedAt:    serverTimestamp(),
          method:       "self-service",
          grantedBy:    "self",
          grantedByEmail: currentUser.email || null,
          campaign:     codeData.campaign || null,
        }));
      } catch (auditErr) {
        // Audit log is non-critical — don't fail the redemption
        console.warn("[V58/redeemFounderCode] audit log failed:", auditErr);
      }

      return {
        ok: true,
        tier,
        founderCode: normalizedCode,
        isLifetime,
        trialDays: codeData.trialDays || null,
        message: isLifetime
          ? "You've unlocked " + (tier === "pro" ? "Pro" : "Premium") + " for life!"
          : "You've unlocked a " + codeData.trialDays + "-day " + (tier === "pro" ? "Pro" : "Premium") + " trial!",
      };
    } catch (e) {
      console.error("[V58/redeemFounderCode] threw:", e);
      if (e && e.code === "permission-denied") {
        return { ok: false, code: "permission-denied", error: "Firestore rules blocked the redemption. Check rules for founder_codes." };
      }
      return { ok: false, code: e.code || "fetch-failed", error: e.message };
    }
  },

  /**
   * V57.7 — List the most recent code_redemptions entries (audit log).
   * Returns { ok, rows: [{id, userEmail, code, tier, method, grantedAt, ...}] }.
   * Defaults to 20 rows. Pass `limit` (capped at 100) to fetch more.
   */
  async adminListRecentRedemptions(limit = 20) {
    if (!currentUser) return { ok: false, code: "admin-not-signed-in" };
    const cap = Math.min(Math.max(1, parseInt(limit, 10) || 20), 100);
    try {
      const q = query(
        collection(db, "code_redemptions"),
        orderBy("grantedAt", "desc"),
        fsLimit(cap)
      );
      const snap = await getDocs(q);
      const rows = [];
      snap.forEach(d => {
        const data = d.data() || {};
        rows.push({
          id: d.id,
          userEmail:        data.userEmail || "",
          userId:           data.userId || "",
          userDisplayName:  data.userDisplayName || "",
          code:             data.code || "",
          codeType:         data.codeType || "",
          tier:             data.tier || "",
          method:           data.method || "",
          grantedBy:        data.grantedBy || "",
          grantedByEmail:   data.grantedByEmail || "",
          reason:           data.reason || "",
          grantedAt:        data.grantedAt && typeof data.grantedAt.toMillis === "function" ? data.grantedAt.toMillis() : null,
        });
      });
      return { ok: true, rows };
    } catch (e) {
      console.error("[V57.7/adminListRecentRedemptions] query threw:", e);
      if (e && e.code === "permission-denied") {
        return { ok: false, code: "permission-denied", error: "Firestore Rules blocked the read. Verify admin rules are deployed." };
      }
      return { ok: false, code: e.code, error: e.message };
    }
  },
};


// ─── 11. Expose to window for the Babel/React script in grounded.html ──────
//
window.groundedAuth = groundedAuth;
window.groundedSync = groundedSync;
window.groundedReady = readyPromise;
