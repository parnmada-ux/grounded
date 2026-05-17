// ============================================================================
//  GROUNDED — Firebase backend module (V15, May 2026)
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
  createUserWithEmailAndPassword,
  signInWithPopup,
  signOut as fbSignOut,
  sendPasswordResetEmail,
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
    lockedRate: null,
    isFounder: false,
    founderNumber: null,
    isLifetime: false,
    comp: null,
    stripeCustomerId: null,
    stripeSubscriptionId: null,
    pricingPlanId: null,
    trialStartDate: serverTimestamp(),
    trialEndDate: Timestamp.fromDate(new Date(Date.now() + TRIAL_DAYS * MS_PER_DAY)),
    paidStartAt: null,            // set by M3 webhook on first invoice.payment_succeeded
    currentPeriodEnd: null,
    cancelAtPeriodEnd: false,
    gracePeriodEndDate: null,
    lastWebhookEventId: null,     // M3 webhook idempotency key — last Stripe event ID applied
    pricingHistory: [],
  };
}

/** Pure read on a Timestamp-or-null field. Returns ms or null. */
function tsMillis(t) {
  return (t && typeof t.toMillis === "function") ? t.toMillis() : null;
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
 */
function computeEffectiveTier(sub) {
  if (!sub) return "pro";                                          // A1
  if (sub.isLifetime) return "premium";

  if (sub.comp === "grace14_v20") {
    const ends = tsMillis(sub.gracePeriodEndDate);
    return (ends !== null && ends > Date.now()) ? "premium" : null;
  }

  if (sub.status === "trial") {
    const ends = tsMillis(sub.trialEndDate);
    if (ends !== null && ends < Date.now()) return null;            // B1
    return "pro";
  }

  if (sub.status === "active" || sub.status === "grace") {
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

async function ensureUserDocAndMigrate(user) {
  const userRef = doc(db, "users", user.uid);
  const snap = await getDoc(userRef);
  const remote = snap.exists() ? snap.data() : null;
  const local = loadAnonData();
  const hasLocal = Object.keys(local).length > 0;

  // System fields we always set/keep.
  const baseUser = {
    email: user.email || "",
    displayName: user.displayName || "",
    photoURL: user.photoURL || "",
    providers: user.providerData.map(p => p.providerId),
  };

  // ── Case C: brand-new user, nothing to migrate
  if (!remote && !hasLocal) {
    await setDoc(userRef, clean({
      schemaVersion: 1,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
      migratedFromLocalStorage: false,
      user: baseUser,
      subscription: defaultTrialSubscription(),     // V24: 7-day trial begins now
    }));
    return { migrated: false, source: "fresh" };
  }

  // ── Case A: upload localStorage as initial doc (one-time V20 hydrate carve-out)
  if (!remote && hasLocal) {
    await setDoc(userRef, clean({
      ...local,
      schemaVersion: 1,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
      migratedFromLocalStorage: true,
      user: { ...(local.user || {}), ...baseUser },
      subscription: defaultTrialSubscription(),     // V24: 7-day trial begins now
    }));
    clearAnonData();
    return { migrated: true, source: "fresh-upload" };
  }

  // ── Case B: merge — remote wins on overlap, local fills gaps
  if (remote && hasLocal) {
    const merged = mergeForMigration(local, remote);
    merged.migratedFromLocalStorage = true;
    merged.updatedAt = serverTimestamp();
    // Keep auth-derived user fields fresh
    merged.user = { ...(merged.user || {}), ...baseUser };
    await setDoc(userRef, clean(merged), { merge: true });
    clearAnonData();
    return { migrated: true, source: "merge" };
  }

  // ── Case D: doc exists, no localStorage
  // Light touch — keep auth-derived user fields fresh on every sign-in.
  await setDoc(userRef, clean({
    user: { ...(remote.user || {}), ...baseUser },
    updatedAt: serverTimestamp(),
  }), { merge: true });
  return { migrated: false, source: "existing" };
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
    try {
      const cred = await signInWithEmailAndPassword(auth, email, password);
      return { ok: true, user: cred.user };
    } catch (e) {
      return { ok: false, code: e.code, error: e.message };
    }
  },

  async signInWithGoogle() {
    try {
      const cred = await signInWithPopup(auth, googleProvider);
      return { ok: true, user: cred.user };
    } catch (e) {
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
   *          OR omitted                — defaults to parnmada-ux.github.io/grounded
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
        const base = opts.returnPath || "https://parnmada-ux.github.io/grounded";
        const sep  = base.includes("?") ? "&" : "?";
        successUrl = successUrl || `${base}${sep}checkout=success`;
        cancelUrl  = cancelUrl  || `${base}${sep}checkout=cancel`;
      }
    } else {
      const base = "https://parnmada-ux.github.io/grounded";
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
   * Open the Stripe Customer Portal so the user can manage their subscription
   * (update card, cancel, view invoices). Stripe-hosted UI. Redirects via
   * window.location.href on success.
   *
   *   opts:  { returnUrl: 'https://...' }  — absolute URL (recommended)
   *          OR { returnPath: '/path' }    — helper resolves to absolute
   *          OR omitted                    — defaults to parnmada-ux.github.io/grounded
   *
   *   404 'no-subscription' means user has no Stripe customer record yet
   *   (they've never completed Checkout).
   */
  async openCustomerPortal(opts) {
    if (!currentUser)         return { ok: false, code: "no-user" };
    if (!workerConfigured())  return { ok: false, code: "worker-not-configured" };

    let returnUrl;
    if (opts && typeof opts === "object") {
      returnUrl = opts.returnUrl || opts.returnPath || "https://parnmada-ux.github.io/grounded";
    } else if (typeof opts === "string") {
      returnUrl = opts;
    } else {
      returnUrl = "https://parnmada-ux.github.io/grounded";
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
};


// ─── 11. Expose to window for the Babel/React script in grounded.html ──────
//
window.groundedAuth = groundedAuth;
window.groundedSync = groundedSync;
window.groundedReady = readyPromise;
