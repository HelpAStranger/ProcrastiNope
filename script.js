import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { 
    getAuth, 
    onAuthStateChanged, 
    GoogleAuthProvider, 
    signInWithPopup, 
    createUserWithEmailAndPassword, 
    signInWithEmailAndPassword, 
    signOut,
    reauthenticateWithCredential,
    EmailAuthProvider,
    updatePassword,
    updateEmail,
    fetchSignInMethodsForEmail,
    deleteUser
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { 
    getFirestore, 
    doc, 
    getDoc, 
    setDoc,
    writeBatch,
    onSnapshot,
    collection,
    query,
    where,
    getDocs,
    updateDoc,
    arrayUnion,
    arrayRemove,
    deleteDoc,
    documentId,
    addDoc,
    initializeFirestore,
    persistentLocalCache,
    memoryLocalCache,
    persistentMultipleTabManager
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";


/*
--- IMPORTANT: FIREBASE SECURITY RULES UPDATE ---
The user has provided updated rules in the prompt. Please ensure your Firebase Console
(Firestore Database -> Rules tab) matches the rules provided in the prompt.
The rules provided in the prompt are:

rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {

    // The 'usernames' collection is used for unique usernames
    match /usernames/{username} {
      allow read; // Anyone can check if a username exists
      allow create: if request.auth != null &&
                    request.resource.data.userId == request.auth.uid;
      allow delete: if request.auth != null &&
                    resource.data.userId == request.auth.uid;
    }

    // friendRequests collection for pending friend connections
    match /friendRequests/{requestId} {
      // A user can create a request if they are the sender.
      allow create: if request.auth != null &&
                    request.resource.data.senderUid == request.auth.uid;

      // Participants can read the request.
      allow read: if request.auth != null &&
                  request.auth.uid in resource.data.participants;

      // The recipient can update the status (to accept/decline).
      allow update: if request.auth != null &&
                    request.auth.uid == resource.data.recipientUid;

      // Either participant can delete the request (sender to cancel, recipient
      // to decline, or either to clean up).
      allow delete: if request.auth != null &&
                    request.auth.uid in resource.data.participants;
    }

    // The 'friendRemovals' collection handles reciprocal friend removals.
    match /friendRemovals/{removalId} {
      // A user can create a removal request if they are the one doing the removing.
      allow create: if request.auth != null &&
                    request.resource.data.removerUid == request.auth.uid;
      // The user being removed can read and delete the request to process it.
      allow read, delete: if request.auth != null && resource.data.removeeUid == request.auth.uid;
    }

    // The 'users' collection stores all private and public data for each user.
    match /users/{userId} {
      // Needed for "login by username": allow fetching a single doc (to get email)
      allow get: if true;

      // Normal profile reads for logged-in users (friends list, etc.)
      allow read: if request.auth != null;

      // A user can create their own user document
      allow create: if request.auth != null && request.auth.uid == userId;

      // Updates should restrict fields to avoid privilege escalation
      allow update: if request.auth != null && request.auth.uid == userId;

      // Only a user can delete their own doc
      allow delete: if request.auth != null && request.auth.uid == userId;
    }

    // Shared quests between friends
    match /sharedQuests/{questId} {
      // Rule for collection queries and single-doc reads. Must match the query structure.
      allow read: if request.auth != null &&
                   request.auth.uid in resource.data.participants;

      // Rule for single-doc updates. Both participants can update their completion status or overall status.
      allow update: if request.auth != null &&
                      (request.auth.uid == resource.data.ownerUid || request.auth.uid == resource.data.friendUid);
      
      // Only the owner can delete a shared quest (e.g., to cancel a pending share or unshare an active one).
      // The friend must 'abandon' or 'reject' it (an update), which triggers the owner's client to delete.
      allow delete: if request.auth != null &&
                      request.auth.uid == resource.data.ownerUid;

      // Only the owner can create a shared quest
      allow create: if request.auth != null &&
                    request.auth.uid == request.resource.data.ownerUid;
    }

    // Shared groups between friends
    match /sharedGroups/{groupId} {
      // Rule for collection queries and single-doc reads. Must match the query structure.
      allow read: if request.auth != null &&
                   request.auth.uid in resource.data.participants;

      // Rule for single-doc updates. Both participants can update, but with restrictions.
      // Owner can update anything. Friend can only update status or their task completions.
      allow update: if request.auth != null &&
                      (request.auth.uid == resource.data.ownerUid || request.auth.uid == resource.data.friendUid);

      // Only the owner can delete a shared group (e.g., to cancel a pending share or unshare an active one).
      // The friend must 'abandon' or 'reject' it (an update), which triggers the owner's client to delete.
      allow delete: if request.auth != null &&
                      request.auth.uid == resource.data.ownerUid;

      // Only the owner can create a shared group
      allow create: if request.auth != null &&
                    request.auth.uid == request.resource.data.ownerUid;
    }
  }
}

Your Firebase config is meant to be public. True security is enforced
by your Firestore Security Rules, not by hiding your API keys.
*/

// --- FIREBASE SETUP ---
const firebaseConfig = {
    apiKey: "AIzaSyAOKGyzZ984TpHBrrgpOvlHKFJlDngGOSM",
    authDomain: "procrastinope.firebaseapp.com",
    projectId: "procrastinope",
    storageBucket: "procrastinope.appspot.com",
    messagingSenderId: "513129540063",
    appId: "1:513129540063:web:5fa30d80d41aa121bffc6a",
    measurementId: "G-5PJTMZFS2C"
};

let app, auth, db;
let currentUser = null;
let unsubscribeFromFirestore = null;
let unsubscribeFromFriendsAndShares = null; // Renamed from unsubscribeFromFriends
let unsubscribeFromSharedQuests = null;
let unsubscribeFromSharedGroups = null;
let appController = null;
let lastFocusedElement = null;
let localDataTimestamp = 0; // To track the timestamp of the currently loaded data.

let activeMobileActionsItem = null; 

// --- DOM ELEMENTS FOR STARTUP ---
const loaderOverlay = document.getElementById('loader-overlay');
const landingPage = document.getElementById('landing-page');
const appWrapper = document.getElementById('app-wrapper');
const landingChoices = document.getElementById('landing-choices');
const landingAuthContainer = document.getElementById('landing-auth-container');

// --- GLOBAL HELPER FUNCTIONS & STATE ---
let settings = { theme: 'system', accentColor: 'var(--accent-red)', volume: 0.3 };
let zIndexCounter = 1000; // Base z-index for modals

/**
 * Manages all audio playback for the application.
 * This architecture is data-driven, using a "soundBank" to define sounds,
 * making it modular and easy to extend. It also correctly handles the
 * AudioContext lifecycle to prevent browser autoplay issues and reduce latency.
 */
const audioManager = {
    audioCtx: null,
    isInitialized: false,

    // The soundBank defines how each sound is generated. Each entry is a function
    // that returns an oscillator, its duration, and a volume multiplier.
    soundBank: {
        'complete': (ctx) => { const o = ctx.createOscillator(); o.type = 'sine'; o.frequency.setValueAtTime(440, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(880, ctx.currentTime + 0.2); return { oscillator: o, duration: 0.2, vol: 1 }; },
        'levelUp': (ctx) => { const o = ctx.createOscillator(); o.type = 'sawtooth'; o.frequency.setValueAtTime(200, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(1000, ctx.currentTime + 0.4); return { oscillator: o, duration: 0.4, vol: 1.2 }; },
        'timerUp': (ctx) => { const o = ctx.createOscillator(); o.type = 'square'; o.frequency.setValueAtTime(880, ctx.currentTime); o.frequency.linearRampToValueAtTime(440, ctx.currentTime + 0.5); return { oscillator: o, duration: 0.5, vol: 1 }; },
        'add': (ctx) => { const o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.setValueAtTime(300, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(600, ctx.currentTime + 0.1); return { oscillator: o, duration: 0.15, vol: 1 }; },
        'addGroup': (ctx) => { const o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.setValueAtTime(300, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(600, ctx.currentTime + 0.1); return { oscillator: o, duration: 0.15, vol: 1 }; },
        'delete': (ctx) => { const o = ctx.createOscillator(); o.type = 'square'; o.frequency.setValueAtTime(200, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(100, ctx.currentTime + 0.1); return { oscillator: o, duration: 0.2, vol: 1 }; },
        'hover': (ctx) => { const o = ctx.createOscillator(); o.type = 'sine'; o.frequency.setValueAtTime(800, ctx.currentTime); return { oscillator: o, duration: 0.05, vol: 0.2 }; },
        'toggle': (ctx) => { const o = ctx.createOscillator(); o.type = 'square'; o.frequency.setValueAtTime(800, ctx.currentTime); o.frequency.exponentialRampToValueAtTime(400, ctx.currentTime + 0.08); return { oscillator: o, duration: 0.1, vol: 0.8 }; },
        'open': (ctx) => { const o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.setValueAtTime(250, ctx.currentTime); o.frequency.linearRampToValueAtTime(500, ctx.currentTime + 0.1); return { oscillator: o, duration: 0.1, vol: 1 }; },
        'close': (ctx) => { const o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.setValueAtTime(500, ctx.currentTime); o.frequency.linearRampToValueAtTime(250, ctx.currentTime + 0.1); return { oscillator: o, duration: 0.1, vol: 1 }; },
        'share': (ctx) => { const o = ctx.createOscillator(); o.type = 'sine'; o.frequency.setValueAtTime(523.25, ctx.currentTime); o.frequency.linearRampToValueAtTime(659.25, ctx.currentTime + 0.15); return { oscillator: o, duration: 0.2, vol: 1 }; },
        'friendComplete': (ctx) => { const o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.setValueAtTime(659.25, ctx.currentTime); o.frequency.linearRampToValueAtTime(880, ctx.currentTime + 0.2); return { oscillator: o, duration: 0.25, vol: 0.8 }; },
        'sharedQuestFinish': (ctx) => { const o = ctx.createOscillator(); o.type = 'sine'; o.frequency.setValueAtTime(523, ctx.currentTime); o.frequency.linearRampToValueAtTime(783, ctx.currentTime + 0.15); o.frequency.linearRampToValueAtTime(1046, ctx.currentTime + 0.4); return { oscillator: o, duration: 0.5, vol: 1.2 }; }
    },

    /**
     * Initializes the AudioContext. Must be called after a user gesture.
     */
    init() {
        if (this.isInitialized || this.audioCtx) return;
        try {
            this.audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            this.isInitialized = true;
            console.log('Audio system initialized.');
        } catch (e) {
            console.error("Web Audio API not supported or could not be created.", e);
        }
    },

    /**
     * Plays a sound defined in the soundBank.
     * @param {string} type The name of the sound to play.
     */
    playSound(type) {
        if (!this.isInitialized || !this.audioCtx) {
            return; // Audio system not ready.
        }

        // If context is suspended, try to resume it. Sound will play on the next interaction.
        if (this.audioCtx.state === 'suspended') {
            this.audioCtx.resume();
            return;
        }
        
        if (settings.volume === 0) return;

        const soundGenerator = this.soundBank[type];
        if (!soundGenerator) {
            console.warn(`Sound type "${type}" not found in sound bank.`);
            return;
        }

        const now = this.audioCtx.currentTime;
        const gainNode = this.audioCtx.createGain();
        gainNode.connect(this.audioCtx.destination);

        const { oscillator, duration, vol } = soundGenerator(this.audioCtx);
        oscillator.connect(gainNode);

        const finalVolume = settings.volume * (vol || 1);

        gainNode.gain.setValueAtTime(0, now);
        gainNode.gain.linearRampToValueAtTime(finalVolume, now + 0.01);
        gainNode.gain.exponentialRampToValueAtTime(0.0001, now + duration);

        oscillator.start(now);
        oscillator.stop(now + duration);
    }
};

// Initialize audio on the first user interaction to comply with autoplay policies.
document.body.addEventListener('click', () => audioManager.init(), { once: true });
document.body.addEventListener('keydown', () => audioManager.init(), { once: true });

function hideActiveTaskActions() {
    if (activeMobileActionsItem) {
        const optionsBtn = activeMobileActionsItem.querySelector('.options-btn');
        if (optionsBtn) optionsBtn.classList.remove('is-active-trigger');
        activeMobileActionsItem.classList.remove('actions-visible');
        activeMobileActionsItem = null;
    }
}

const openModal = (modal) => {
    if(modal) {
        lastFocusedElement = document.activeElement;
        hideActiveTaskActions();

        // NEW: Increment and apply z-index to ensure new modals open on top.
        zIndexCounter++;
        modal.style.zIndex = zIndexCounter;

        appWrapper.classList.add('blur-background');
        modal.classList.add('visible');
        audioManager.playSound('open');
        // Focus the first focusable element inside the modal
        const focusableElements = modal.querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])');
        const firstFocusable = Array.from(focusableElements).find(el => el.offsetParent !== null);
        if (firstFocusable) {
            setTimeout(() => firstFocusable.focus(), 100);
        }
    }
};
const closeModal = (modal) => {
    if(modal) {
        // Only remove the blur if this is the last modal being closed.
        const visibleModals = document.querySelectorAll('.modal-overlay.visible');
        if (visibleModals.length <= 1) {
            appWrapper.classList.remove('blur-background');
        }

        modal.classList.remove('visible');
        audioManager.playSound('close');
        if (lastFocusedElement) {
            lastFocusedElement.focus();
        }
    }
};

// --- Initialize Firebase and start the auth flow ---
try {
    app = initializeApp(firebaseConfig);
    auth = getAuth(app);

    // Initialize Firestore with offline persistence.
    // This must be done before any other Firestore operations.
    try {
        db = initializeFirestore(app, { cache: persistentLocalCache({ tabManager: persistentMultipleTabManager() }) });
    } catch (err) {
        if (err.code === 'failed-precondition' || err.code === 'unimplemented') {
            const reason = err.code === 'failed-precondition' ? 'Multiple tabs open' : 'Browser not supported';
            console.warn(`Firestore persistence failed: ${reason}. App will work online only.`);
            db = getFirestore(app); // Fallback to online-only
        }
    }
    
    onAuthStateChanged(auth, async (user) => {
        // Cleanup previous user's data listeners to prevent memory leaks.
        if (unsubscribeFromFirestore) {
            unsubscribeFromFirestore(); 
            unsubscribeFromFirestore = null;
        }
        if (unsubscribeFromFriendsAndShares) {
            unsubscribeFromFriendsAndShares();
            unsubscribeFromFriendsAndShares = null;
        }
        if (unsubscribeFromSharedQuests) {
            unsubscribeFromSharedQuests();
            unsubscribeFromSharedQuests = null;
        }
        if (unsubscribeFromSharedGroups) {
            unsubscribeFromSharedGroups();
            unsubscribeFromSharedGroups = null;
        }
        
        currentUser = user;
        
        if (user) {
            // A user is logged in.
            loaderOverlay.style.display = 'none';
            landingPage.style.display = 'none';
            appWrapper.style.display = 'block';

            // ALWAYS check for guest data to merge upon login.
            const guestDataString = localStorage.getItem('anonymousUserData');
            if (guestDataString) {
                try {
                    const userDocRef = doc(db, "users", user.uid);
                    const docSnap = await getDoc(userDocRef);
                    const cloudData = docSnap.exists() && docSnap.data().appData ? docSnap.data().appData : {};
                    const mergedData = mergeGuestDataWithCloud(cloudData);
                    await setDoc(userDocRef, { appData: mergedData }, { merge: true });
                    localStorage.removeItem('anonymousUserData');
                    sessionStorage.removeItem('isGuest'); // Also clear guest session flag
                } catch (mergeError) {
                    console.error("Failed to merge guest data on login:", mergeError);
                }
            }

            if (!appController) {
                appController = await initializeAppLogic(user); 
            } else {
                await appController.updateUser(user);
            }
            appWrapper.classList.add('loaded'); // Trigger load animations
        } else { 
            // No user is logged in.
            if (sessionStorage.getItem('isGuest')) {
                loaderOverlay.style.display = 'none';
                landingPage.style.display = 'none';
                appWrapper.style.display = 'block';
                if (!appController) appController = await initializeAppLogic(null);
                appWrapper.classList.add('loaded'); // Trigger load animations
            } else {
                loaderOverlay.style.display = 'none';
                landingPage.style.display = 'flex';
                appWrapper.style.display = 'none';
                if(appController) appController.shutdown();
                appController = null;
                appWrapper.classList.remove('loaded'); // Reset on logout
            }
        }
    });
} catch (err) {
    console.error("Firebase initialization failed:", err);
    loaderOverlay.innerHTML = '<p style="color: var(--text);">Error: Could not connect. Please check your Firebase config.</p>';
}

// --- LANDING PAGE / AUTH FLOW ---
function showLandingPage() {
    landingAuthContainer.style.display = 'none';
    landingChoices.style.display = 'block';
}

document.getElementById('landing-guest-btn').addEventListener('click', async () => {
    sessionStorage.setItem('isGuest', 'true');
    loaderOverlay.style.display = 'flex';
    if (!appController) {
        appController = await initializeAppLogic(null);
    }
    landingPage.style.display = 'none';
    appWrapper.style.display = 'block';
    loaderOverlay.style.display = 'none';
});

document.getElementById('landing-login-btn').addEventListener('click', () => {
    showAuthFormsOnLanding('login');
});

function showAuthFormsOnLanding(initialTab) {
    landingChoices.style.display = 'none';
    landingAuthContainer.style.display = 'block';
    
    const onAuthSuccess = () => {};
    
    setupAuthForms(landingAuthContainer, onAuthSuccess);
    
    landingAuthContainer.querySelector(`.toggle-btn[data-tab="${initialTab}"]`).click();
    
    if (!landingAuthContainer.querySelector('#landing-back-btn')) {
        const backBtn = document.createElement('button');
        backBtn.id = 'landing-back-btn';
        backBtn.className = 'btn';
        backBtn.textContent = 'Back';
        backBtn.onclick = showLandingPage;
        landingAuthContainer.appendChild(backBtn);
    }
}

// --- MAIN APPLICATION LOGIC ---
async function initializeAppLogic(initialUser) {

    const focusOnDesktop = (el) => {
        // FIX: Check for touch support more reliably. 'pointer: coarse' can be true for touch-screen laptops.
        const isTouchDevice = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
        if (!isTouchDevice && el) {
            el.focus();
        }
    };

    let user = initialUser;

    let lastSection = 'daily';

    let dailyTasks = [], standaloneMainQuests = [], generalTaskGroups = [], sharedQuests = [], incomingSharedItems = [], incomingFriendRequests = [], outgoingFriendRequests = [];
    let isMultiSelectModeActive = false;
    let selectedQuestIds = new Set();
    let sharedGroups = [];
    let allSharedGroupsFromListener = [];
    let questsMap = new Map(); // Make it available in the broader scope
    let confirmedFriendUIDs = [];
    let playerData = { level: 1, xp: 0 };
    let currentListToAdd = null, currentEditingTaskId = null, currentEditingGroupId = null;
    // PERF: Refactored timers to use CSS transitions instead of a JS loop.
    const lastClickTimes = new Map();
    const CLICK_DEBOUNCE_TIME = 250; // ms, to prevent double-clicks
    let activeTimers = new Map(); // Map<taskId, timeoutId> to manage timer completion.
    let undoTimeoutMap = new Map();
    let shiftHoverItem = null; // To track items whose actions are shown via shift-hover

    let lastPotentialShiftHoverItem = null; // To track the item under the mouse for shift-hover

    const debouncedRenderFriends = debounce(renderFriendsList, 100);
    
    const sharedQuestsContainer = document.getElementById('shared-quests-container');
    const dailyTaskListContainer = document.getElementById('daily-task-list');
    const standaloneTaskListContainer = document.getElementById('standalone-task-list');
    const generalTaskListContainer = document.getElementById('general-task-list-container');
    const playerLevelEl = document.getElementById('player-level');
    const questsLayout = document.querySelector('.quests-layout');
    const xpBarEl = document.getElementById('xp-bar');
    const xpTextEl = document.getElementById('xp-text');
    const levelDisplayEl = document.querySelector('.level-display');
    const addTaskTriggerBtnDaily = document.querySelector('.add-task-trigger-btn[data-list="daily"]');
    const addStandaloneTaskBtn = document.getElementById('add-standalone-task-btn');
    const addGroupBtn = document.getElementById('add-group-btn');
    
    const addTaskModal = document.getElementById('add-task-modal');
    const addTaskModalTitle = document.getElementById('add-task-modal-title');
    const addTaskForm = document.getElementById('add-task-form');
    const newTaskInput = document.getElementById('new-task-input');
    
    const editTaskModal = document.getElementById('edit-task-modal');
    const editTaskForm = document.getElementById('edit-task-form');
    const editTaskInput = document.getElementById('edit-task-input');
    const editTaskIdInput = document.getElementById('edit-task-id');
    const editWeeklyGoalContainer = document.getElementById('edit-weekly-goal-container');

    const weeklyGoalContainer = document.getElementById('weekly-goal-container');
    const weeklyGoalSlider = document.getElementById('new-task-weekly-goal-slider');
    const weeklyGoalDisplay = document.getElementById('new-task-weekly-goal-display');
    const editWeeklyGoalSlider = document.getElementById('edit-task-weekly-goal-slider');
    const editWeeklyGoalDisplay = document.getElementById('edit-task-weekly-goal-display');

    const addGroupModal = document.getElementById('add-group-modal');
    const addGroupForm = document.getElementById('add-group-form');
    const newGroupInput = document.getElementById('new-group-input');
    const timerModal = document.getElementById('timer-modal');
    const timerForm = document.getElementById('timer-form');
    const timerDurationSlider = document.getElementById('timer-duration-slider');
    const timerDurationDisplay = document.getElementById('timer-duration-display');
    const timerUnitSelector = document.querySelector('.timer-unit-selector');
    const timerMenuModal = document.getElementById('timer-menu-modal');
    const timerMenuCancelBtn = document.getElementById('timer-menu-cancel-btn');
    const settingsBtn = document.getElementById('settings-btn');
    const settingsModal = document.getElementById('settings-modal');
    const themeOptionsButtons = document.getElementById('theme-options-buttons');
    const colorOptions = document.getElementById('color-options');
    const volumeSlider = document.getElementById('volume-slider');
    const resetProgressBtn = document.getElementById('reset-progress-btn');
    const exportDataBtn = document.getElementById('export-data-btn');
    const importDataBtn = document.getElementById('import-data-btn');
    const importFileInput = document.getElementById('import-file-input');
    const guestDataManagementGroup = document.getElementById('guest-data-management');
    const settingsLoginBtn = document.getElementById('settings-login-btn');
    const manageAccountBtn = document.getElementById('manage-account-btn');
    const logoutBtn = document.getElementById('logout-btn');
    const userDisplay = document.getElementById('user-display');
    const accountModal = document.getElementById('account-modal');
    const manageAccountModal = document.getElementById('manage-account-modal');
    const confirmModal = document.getElementById('confirm-modal');
    const confirmActionBtn = document.getElementById('confirm-action-btn');
    const confirmCancelBtn = document.getElementById('confirm-cancel-btn');
    const confirmTitle = document.getElementById('confirm-title');
    const confirmText = document.getElementById('confirm-text');
    const noDailyTasksMessage = document.getElementById('no-daily-tasks-message');
    const noGeneralTasksMessage = document.getElementById('no-general-tasks-message');
    const quoteEl = document.getElementById('quote-of-the-day');
    const offlineIndicator = document.getElementById('offline-indicator');
    let confirmCallback = null;
    
    const friendsBtnDesktop = document.getElementById('friends-btn-desktop');
    const friendsModal = document.getElementById('friends-modal');
    const mobileNav = document.getElementById('mobile-nav');
    const addFriendForm = document.getElementById('add-friend-form');
    const searchUsernameInput = document.getElementById('search-username-input');
    const friendStatusMessage = friendsModal.querySelector('.friend-status-message');
    const friendRequestCountBadge = document.getElementById('friend-request-count');
    const friendRequestCountBadgeMobile = document.getElementById('friend-request-count-mobile');
    const friendRequestCountBadgeModal = document.getElementById('friend-request-count-modal');
    const friendsListContainer = friendsModal.querySelector('.friends-list-container');
    const friendRequestsContainer = friendsModal.querySelector('.friend-requests-container');
    const deleteAccountBtn = document.getElementById('delete-account-btn');

    const resetCloudDataBtn = document.getElementById('reset-cloud-data-btn');
    const shareQuestModal = document.getElementById('share-quest-modal');
    const shareQuestFriendList = document.getElementById('share-quest-friend-list');
    const shareQuestIdInput = document.getElementById('share-quest-id-input');

    // NEW: DOM elements for Share Group feature
    const shareGroupModal = document.getElementById('share-group-modal');
    const shareGroupNameDisplay = document.getElementById('share-group-name-display');
    const shareGroupIdInput = document.getElementById('share-group-id-input');
    const shareGroupFriendList = document.getElementById('share-group-friend-list');

    // NEW: DOM elements for Multi-Select
    const multiSelectToggleBtns = document.querySelectorAll('.multi-select-toggle-btn');
    const batchActionsModal = document.getElementById('batch-actions-modal');
    const batchActionsModalCounter = document.getElementById('batch-actions-modal-counter');
    const batchModalCompleteBtn = document.getElementById('batch-modal-complete-btn');
    const batchModalUncompleteBtn = document.getElementById('batch-modal-uncomplete-btn');
    const batchModalTimerBtn = document.getElementById('batch-modal-timer-btn');
    const batchModalShareBtn = document.getElementById('batch-modal-share-btn');
    const batchModalUnshareBtn = document.getElementById('batch-modal-unshare-btn');
    const batchModalDeleteBtn = document.getElementById('batch-modal-delete-btn');

    // NEW: DOM elements for Shares tab
    const sharesTabContent = document.getElementById('shares-tab'); // eslint-disable-line no-unused-vars
    const incomingSharesContainer = sharesTabContent.querySelector('.incoming-shares-container');
    const sharesRequestCountBadge = document.getElementById('shares-request-count-modal');
    const friendsModalToggle = friendsModal.querySelector('.form-toggle');


    async function promptForUsernameIfNeeded() {
        if (!user) return;

        const userDocRef = doc(db, "users", user.uid);
        const docSnap = await getDoc(userDocRef);
        const existingData = docSnap.exists() ? docSnap.data().appData : null;
        
        // Check if username is missing or if the user document itself doesn't exist
        if (!docSnap.exists() || !docSnap.data()?.username) {
            const usernameModal = document.getElementById('username-modal');
            const usernameForm = document.getElementById('username-form');
            const newUsernameInput = document.getElementById('new-username-input');
            const usernameErrorEl = usernameModal.querySelector('.username-error');
            
            usernameModal.setAttribute('data-persistent', 'true');

            openModal(usernameModal);
            focusOnDesktop(newUsernameInput);

            return new Promise((resolve) => {
                usernameForm.onsubmit = async (e) => {
                    e.preventDefault();
                    usernameErrorEl.textContent = '';
                    const username = newUsernameInput.value.trim().toLowerCase();
                    if (!username || username.length < 3) {
                        usernameErrorEl.textContent = 'Username must be at least 3 characters.';
                        return;
                    }

                    const submitButton = usernameForm.querySelector('button[type="submit"]');
                    submitButton.disabled = true;
                    submitButton.textContent = 'Saving...';
                    
                    try {
                        const usernamesRef = doc(db, "usernames", username);
                        const usernameSnap = await getDoc(usernamesRef);

                        if (usernameSnap.exists()) {
                            throw new Error('This username is already taken.');
                        }

                        const batch = writeBatch(db);
                        batch.set(usernamesRef, { userId: user.uid });
                        
                        batch.set(userDocRef, { 
                            username: username, 
                            email: user.email,
                            appData: existingData || {},
                            friends: [],
                            friendRequests: []
                        }, { merge: true });
                        await batch.commit();
                        
                        usernameModal.removeAttribute('data-persistent');
                        closeModal(usernameModal);
                        resolve();

                    } catch (error) {
                        usernameErrorEl.textContent = error.message || getCoolErrorMessage(error);
                    } finally {
                        submitButton.disabled = false;
                        submitButton.textContent = 'Save Username';
                    }
                };
            });
        }
    }
    
    function updateMainNotificationBadges() {
        const requestCount = incomingFriendRequests.length;
        const sharesCount = incomingSharedItems.length;
        const totalCount = requestCount + sharesCount;

        const badges = [friendRequestCountBadge, friendRequestCountBadgeMobile];
        badges.forEach(badge => {
            if (badge) { // Defensive check
                if (totalCount > 0) {
                    badge.textContent = totalCount;
                    badge.style.display = 'flex';
                } else {
                    badge.style.display = 'none';
                }
            }
        });
    }

    function debounce(func, delay) { // MODIFIED: Added a cancel method
        let timeout;
        const debounced = function(...args) {
            const context = this;
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(context, args), delay);
        };
        debounced.cancel = function() {
            clearTimeout(timeout);
        };
        return debounced;
    }

    async function saveData(data) {
        if (data.settings && data.settings.theme) {
            localStorage.setItem('userTheme', data.settings.theme);
        }

        // Add a timestamp to the data payload to resolve multi-device conflicts.
        const dataWithTimestamp = {
            ...data,
            lastModified: Date.now()
        };

        if (!user) {
            localStorage.setItem('anonymousUserData', JSON.stringify(dataWithTimestamp));
            return;
        }
        
        try {
            const userDocRef = doc(db, "users", user.uid);
            // By including the timestamp, we can check on the client-side if the data
            // we receive from a snapshot is newer than what we currently have.
            await setDoc(userDocRef, { appData: dataWithTimestamp }, { merge: true });
        } catch (error) { 
            console.error("Error saving data to Firestore: ", getCoolErrorMessage(error)); 
        }
    }
    
    const debouncedSaveData = debounce(saveData, 1500);

    const saveState = () => {
        // Create a version of generalTaskGroups without the isExpanded property for saving
        const groupsToSave = generalTaskGroups.map(({ isExpanded, ...rest }) => rest);

        const data = { 
            dailyTasks, 
            standaloneMainQuests, 
            generalTaskGroups: groupsToSave, // Use the cleaned version
            playerData, 
            settings 
        };
        if (!user) {
            saveData(data);
        } else {
            debouncedSaveData(data);
        }
    };

    function loadAndDisplayData(data) {
        // Store the current expanded state of groups before loading new data
        const expandedGroupIds = new Set();
        if (Array.isArray(generalTaskGroups)) {
            generalTaskGroups.forEach(g => {
                if (g.isExpanded) {
                    expandedGroupIds.add(g.id);
                }
            });
        }
    
        // Load persisted data
        dailyTasks = data.dailyTasks || [];
        standaloneMainQuests = data.standaloneMainQuests || [];
        generalTaskGroups = data.generalTaskGroups || [];
        playerData = data.playerData || { level: 1, xp: 0 };
        settings = { ...settings, ...(data.settings || {}) }; 
        // Store the timestamp of the data we're loading.
        localDataTimestamp = data.lastModified || 0;
        
        // Re-apply the transient state to the newly loaded data
        generalTaskGroups.forEach(group => {
            if (expandedGroupIds.has(group.id)) {
                group.isExpanded = true;
            } else {
                group.isExpanded = false;
            }
        });
        
        applySettings();
        renderAllLists();
        updateProgressUI();
    }

    async function initialLoad() {
        return new Promise((resolve) => {
            if (!user) {
                const localData = JSON.parse(localStorage.getItem('anonymousUserData')) || {};
                loadAndDisplayData(localData);
                resolve();
                return;
            }
            
            // Listen for friend requests and incoming shared quests
            listenForFriendsAndShares(); 
            // Listen for active shared quests (those accepted by both)
            listenForSharedQuests();
            // Listen for active shared groups
            listenForSharedGroups();

            const userDocRef = doc(db, "users", user.uid);
            let isFirstLoad = true;
            unsubscribeFromFirestore = onSnapshot(userDocRef, (docSnap) => {
                // This block handles real-time updates from Firestore.
                if (docSnap.exists() && docSnap.data().appData) {
                    const incomingData = docSnap.data().appData;
                    const incomingTimestamp = incomingData.lastModified || 0;

                    // CONFLICT RESOLUTION: Only update the local state if the incoming data is newer.
                    // This prevents overwriting local changes that are in the process of being saved,
                    // especially important with offline persistence and multi-device use.
                    // On first load, we always load the data.
                    if (isFirstLoad || incomingTimestamp > localDataTimestamp) {
                        loadAndDisplayData(incomingData);
                    } else if (incomingTimestamp < localDataTimestamp) {
                        console.warn("Stale data from Firestore snapshot ignored.");
                    }
                } else {
                    // This case handles a new user or a user with no data.
                    loadAndDisplayData({});
                }
                if (isFirstLoad) {
                    isFirstLoad = false;
                    resolve();
                }
            }, (error) => {
                console.error("Error listening to Firestore:", getCoolErrorMessage(error));
                if (isFirstLoad) {
                     isFirstLoad = false;
                     resolve();
                }
            });
        });
    }

    async function updateUserUI() {
        if (user) {
            settingsLoginBtn.style.display = 'none';
            logoutBtn.style.display = 'inline-flex';
            manageAccountBtn.style.display = 'inline-flex';
            guestDataManagementGroup.style.display = 'none';

            const userDocRef = doc(db, "users", user.uid);
            const docSnap = await getDoc(userDocRef);
            const username = docSnap.exists() && docSnap.data().username ? docSnap.data().username : user.email;

            userDisplay.textContent = `Logged in as: ${username}`;
            userDisplay.style.display = 'flex';
            
            mobileNav.querySelector('[data-section="friends"]').style.display = 'flex';

        } else {
            settingsLoginBtn.style.display = 'inline-flex';
            logoutBtn.style.display = 'none';
            manageAccountBtn.style.display = 'none';
            guestDataManagementGroup.style.display = 'block';
            userDisplay.textContent = 'Playing as Guest';
            userDisplay.style.display = 'flex';
            
            mobileNav.querySelector('[data-section="friends"]').style.display = 'none';
        }
    }
    const XP_PER_TASK = 35;
    const XP_PER_SHARED_QUEST = 50;
    const XP_PER_TIMER_MINUTE = 2;
    const getXpForNextLevel = (level) => 50 + (level * 50);
    const quotes = ["The secret of getting ahead is getting started.", "A year from now you may wish you had started today.", "The future depends on what you do today."];
    function showRandomQuote() { quoteEl.textContent = `"${quotes[Math.floor(Math.random() * quotes.length)]}"`; }
    function getStartOfWeek(date) {
        const d = new Date(date);
        const day = d.getDay();
        const diff = d.getDate() - day + (day === 0 ? -6 : 1);
        return new Date(d.setDate(diff)).setHours(0, 0, 0, 0);
    }
    const checkDailyReset = () => {
        const today = new Date().toDateString();
        const lastVisit = localStorage.getItem('lastVisitDate');
        if (today !== lastVisit) {
            const yesterday = new Date(Date.now() - 86400000).toDateString();
            dailyTasks.forEach(task => {
                // Only reset non-shared tasks automatically
                if(task.isShared) return; 
                if (task.completedToday && task.lastCompleted === yesterday) task.streak = (task.streak || 0) + 1;
                else if (!task.completedToday) task.streak = 0;
                task.completedToday = false;
                delete task.timerFinished; // Clear timer finished state on reset
                // FIX: Also clear timer properties to prevent it from resuming on a new day.
                delete task.timerStartTime;
                delete task.timerDuration;
            });
            localStorage.setItem('lastVisitDate', today);
            saveState();
        }
    };
    function addXp(amount) {
        playerData.xp += Math.round(amount);
        if (playerData.xp < 0) playerData.xp = 0;
        const requiredXp = getXpForNextLevel(playerData.level);

        // NEW: Animate the XP bar on gain
        if (amount > 0) {
            const container = xpBarEl.parentElement;
            container.classList.remove('xp-gained');
            // Force a reflow to restart the animation if the class is re-added
            void container.offsetWidth;
            container.classList.add('xp-gained');
        }

        if (playerData.xp >= requiredXp) levelUp(requiredXp);
        updateProgressUI();
    }
    function levelUp(requiredXp) {
        playerData.level++;
        playerData.xp -= requiredXp;
        audioManager.playSound('levelUp');
        levelDisplayEl.classList.add('level-up');
        levelDisplayEl.addEventListener('animationend', () => levelDisplayEl.classList.remove('level-up'), { once: true });
        const newRequiredXp = getXpForNextLevel(playerData.level);
        if (playerData.xp >= newRequiredXp) levelUp(newRequiredXp);
    }
    function updateProgressUI() {
        const requiredXp = getXpForNextLevel(playerData.level);
        const progressPercent = Math.min((playerData.xp / requiredXp) * 100, 100);
        playerLevelEl.textContent = playerData.level;
        xpTextEl.textContent = `${Math.floor(playerData.xp)} / ${requiredXp} XP`;
        xpBarEl.style.width = `${progressPercent}%`;
    }
    function checkAllTasksCompleted() {
        // Only consider non-shared tasks for "all tasks completed" logic
        const allDailiesDone = dailyTasks.length > 0 && dailyTasks.filter(t => !t.isShared).every(t => t.completedToday);
        const noStandaloneQuests = standaloneMainQuests.filter(t => !t.isShared).length === 0;
        const noGroupedQuests = generalTaskGroups.every(g => !g.tasks || g.tasks.filter(t => !t.isShared).length === 0);
        return { allDailiesDone, allTasksDone: allDailiesDone && noStandaloneQuests && noGroupedQuests };
    }
    
    const renderSharedItems = () => {
        sharedQuestsContainer.innerHTML = '';
        
        // 1. Render Shared Groups
        sharedGroups.forEach(group => {
            const groupEl = createSharedGroupElement(group);
            sharedQuestsContainer.appendChild(groupEl);
        });
    
        // 2. Render Individual Shared Quests
        const individualQuests = sharedQuests.filter(q => !q.sharedGroupName);
        if (individualQuests.length > 0) {
            const groupEl = document.createElement('div');
            groupEl.className = 'shared-quest-group';
            groupEl.innerHTML = `<h3 class="shared-group-title">Individual Shared Quests</h3>`;
            const ul = document.createElement('ul');
            ul.className = 'shared-quest-list';
            individualQuests.forEach(task => ul.appendChild(createTaskElement(task, 'shared')));
            groupEl.appendChild(ul);
            sharedQuestsContainer.appendChild(groupEl);
        }
    };
    
    // FIX: Updated rendering functions to correctly display tasks based on data
    const renderDailyTasks = () => { 
        dailyTaskListContainer.innerHTML = '';
        // Only render daily tasks that are NOT associated with an active or completed shared quest.
        // The placeholder for pending shares should still be rendered.
        const tasksToRender = dailyTasks.filter(task => {
            if (!task.isShared) return true; // Render normal tasks
            // If shared, only render if it's still pending (i.e., not in the active/completed `sharedQuests` list)
            return !sharedQuests.some(sq => sq.id === task.sharedQuestId);
        });

        tasksToRender.forEach(task => dailyTaskListContainer.appendChild(createTaskElement(task, 'daily')));
        noDailyTasksMessage.style.display = tasksToRender.length === 0 ? 'block' : 'none';
    };
    const renderStandaloneTasks = () => { 
        standaloneTaskListContainer.innerHTML = '';
        const tasksToRender = standaloneMainQuests.filter(task => {
            if (!task.isShared) return true;
            return !sharedQuests.some(sq => sq.id === task.sharedQuestId);
        });
        tasksToRender.forEach(task => standaloneTaskListContainer.appendChild(createTaskElement(task, 'standalone')));
    };
    const renderGeneralTasks = () => { 
        generalTaskListContainer.innerHTML = ''; 
        generalTaskGroups.forEach(group => {
            const el = createGroupElement(group);
            generalTaskListContainer.appendChild(el);
        });

        const hasVisibleStandalone = standaloneMainQuests.some(task => {
            if (!task.isShared) return true;
            return !sharedQuests.some(sq => sq.id === task.sharedQuestId);
        });

        const hasVisibleGrouped = generalTaskGroups.some(g =>
            g.tasks && g.tasks.some(task => {
                if (!task.isShared) return true;
                return !sharedQuests.some(sq => sq.id === task.sharedQuestId);
            })
        );

        noGeneralTasksMessage.style.display = (hasVisibleStandalone || hasVisibleGrouped) ? 'none' : 'block';
    };
    const createGroupElement = (group) => {
        const el = document.createElement('div'); el.className = 'main-quest-group'; if (group.isExpanded) el.classList.add('expanded'); el.dataset.groupId = group.id;
        if (selectedQuestIds.has(group.id)) {
            el.classList.add('multi-select-selected');
        }
        
        if (group.isShared) {
            const sharedGroupData = allSharedGroupsFromListener.find(sg => sg.id === group.sharedGroupId);
            const otherParticipant = sharedGroupData ? sharedGroupData.participants.find(p => p !== user.uid) : null;
            const isOrphan = !sharedGroupData || (otherParticipant && !confirmedFriendUIDs.includes(otherParticipant));

            if (isOrphan) {
                el.classList.add('is-shared-task'); // for styling
                el.innerHTML = `<header class="main-quest-group-header" style="cursor: default;">
                                    <div class="group-title-container">
                                        <h3 style="font-style: italic;">${group.name} (Orphaned)</h3>
                                    </div>
                                    <button class="btn cleanup-orphan-group-btn" data-group-id="${group.id}" data-shared-group-id="${group.sharedGroupId || ''}">Clean Up</button>
                                </header>`;
            } else if (sharedGroupData && sharedGroupData.status === 'pending') {
                el.classList.add('is-shared-task', 'pending-share');
                el.innerHTML = `<header class="main-quest-group-header" style="cursor: default;">
                                    <div class="group-title-container">
                                        <h3 style="font-style: italic;">${group.name}</h3>
                                    </div>
                                    <button class="btn icon-btn cancel-share-group-btn" data-shared-group-id="${group.sharedGroupId}" aria-label="Cancel Share" title="Cancel Share"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line><line x1="1" y1="1" x2="23" y2="23" style="stroke: var(--accent-red); stroke-width: 3px;"></line></svg></button>
                                </header>`;
            } else {
                el.classList.add('is-shared-task'); // Reuse styling for a disabled look
                el.innerHTML = `<header class="main-quest-group-header" style="cursor: default;">
                                    <div class="group-title-container">
                                        <h3 style="font-style: italic;">${group.name}</h3>
                                    </div>
                                    <button class="btn view-shared-group-btn" data-shared-group-id="${group.sharedGroupId || ''}">View Share</button>
                                </header>`;
            }
            return el;
        }

        el.innerHTML = `<header class="main-quest-group-header"><div class="multi-select-checkbox"></div><div class="group-title-container"><svg class="expand-indicator" viewBox="0 0 24 24" fill="currentColor"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18L8.59,16.58Z"/></svg><h3>${group.name}</h3></div><div class="task-actions-container"><button class="btn icon-btn options-btn" aria-label="More options"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg></button></div><div class="group-actions"><button class="btn icon-btn share-group-btn" aria-label="Share group" aria-haspopup="dialog" aria-controls="share-group-modal"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line></svg></button><button class="btn icon-btn edit-group-btn" aria-label="Edit group name" aria-haspopup="dialog" aria-controls="add-group-modal"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button><button class="btn icon-btn delete-group-btn" aria-label="Delete group"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button><button class="btn icon-btn add-task-to-group-btn" aria-label="Add task" aria-haspopup="dialog" aria-controls="add-task-modal"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg></button></div></header><ul class="task-list-group" data-group-id="${group.id}"></ul>`;
        const ul = el.querySelector('ul'); 
        const tasksToRender = group.tasks.filter(task => {
            if (!task.isShared) return true;
            return !sharedQuests.some(sq => sq.id === task.sharedQuestId);
        });
        tasksToRender.forEach(task => ul.appendChild(createTaskElement(task, 'group')));
        return el;
    };
    const createSharedGroupElement = (group) => {
        const groupEl = document.createElement('div');
        // Re-use main-quest-group for structure and styling
        groupEl.className = 'main-quest-group shared-quest-group';
        if (group.isExpanded) groupEl.classList.add('expanded');
        groupEl.dataset.sharedGroupId = group.id;
    
        const allCompleted = group.tasks.every(t => t.ownerCompleted && t.friendCompleted);
        const optionsBtnDisabled = allCompleted ? 'disabled' : '';
        if (allCompleted) {
            groupEl.classList.add('all-completed');
        }
    
        const isOwner = user.uid === group.ownerUid;
        const otherPlayerUsername = isOwner ? group.friendUsername : group.ownerUsername;

        const editBtnHTML = isOwner ? `<button class="btn icon-btn edit-group-btn" aria-label="Edit group name"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>` : '';
        const addTaskBtnHTML = `<button class="btn icon-btn add-task-to-group-btn" aria-label="Add task"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg></button>`;
        const unshareBtnHTML = isOwner ? `<button class="btn icon-btn unshare-group-btn" data-shared-group-id="${group.id}" aria-label="Unshare Group" title="Unshare Group"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line><line x1="1" y1="1" x2="23" y2="23" style="stroke: var(--accent-red); stroke-width: 3px;"></line></svg></button>` : '';
        const abandonBtnHTML = !isOwner ? `<button class="btn icon-btn abandon-group-btn" data-shared-group-id="${group.id}" aria-label="Abandon Group" title="Abandon Group"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg></button>` : '';

        const headerHTML = `
            <header class="main-quest-group-header">
                <div class="group-title-container">
                    <svg class="expand-indicator" viewBox="0 0 24 24" fill="currentColor"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18L8.59,16.58Z"/></svg>
                    <div class="group-title-and-subtitle">
                        <h3>${group.name}</h3>
                        <span class="shared-with-tag">with ${otherPlayerUsername}</span>
                    </div>
                </div><div class="task-actions-container"><button class="btn icon-btn options-btn" aria-label="More options" ${optionsBtnDisabled}><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg></button></div><div class="group-actions">
                    ${editBtnHTML}
                    ${unshareBtnHTML}
                    ${abandonBtnHTML}
                    ${addTaskBtnHTML}
                </div>
            </header>
        `;
        groupEl.innerHTML = headerHTML;

        const ul = document.createElement('ul');
        ul.className = 'task-list-group shared-quest-list';
        group.tasks.forEach(task => {
            ul.appendChild(createSharedTaskElement(task, group));
        });
        groupEl.appendChild(ul);
        return groupEl;
    };
    const createSharedTaskElement = (task, group) => {
        const li = document.createElement('li');
        li.className = 'task-item shared-group-task';
        li.dataset.id = task.id;
        li.dataset.sharedGroupId = group.id;

        const myPartCompleted = user.uid === group.ownerUid ? task.ownerCompleted : task.friendCompleted;
        const optionsBtnDisabled = myPartCompleted ? 'disabled' : '';

        const buttonsHTML = `
            <button class="btn icon-btn timer-clock-btn" aria-label="Set Timer" aria-haspopup="dialog" aria-controls="timer-modal"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><svg class="progress-ring" viewBox="0 0 24 24"><circle class="progress-ring-circle" r="10" cx="12" cy="12"/></svg></button>
            <button class="btn icon-btn edit-btn" aria-label="Edit Quest" aria-haspopup="dialog" aria-controls="edit-task-modal"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>
            <button class="btn icon-btn delete-btn" aria-label="Delete Quest"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>
        `;

        li.innerHTML = `
            <div class="multi-select-checkbox"></div><div class="completion-indicator"></div>
            <div class="task-content">
                <span class="task-text">${task.text}</span>
            </div>
            <div class="task-buttons-wrapper">${buttonsHTML}</div><div class="task-actions-container"><button class="btn icon-btn options-btn" aria-label="More options" ${optionsBtnDisabled}><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg></button></div>
            <div class="shared-status-indicators" title="${group.ownerUsername} | ${group.friendUsername}">
                <div class="status-indicator ${task.ownerCompleted ? 'completed' : ''}"></div>
                <div class="status-indicator ${task.friendCompleted ? 'completed' : ''}"></div>
            </div>`;
        
        if (myPartCompleted) {
            li.classList.add('my-part-completed');
        }
        return li;
    };
    const createTaskElement = (task, type) => {
        const li = document.createElement('li'); li.className = 'task-item'; li.dataset.id = task.id; if (type === 'standalone') li.classList.add('standalone-quest');
        if (selectedQuestIds.has(task.id)) {
            li.classList.add('multi-select-selected');
        }
        let optionsBtnDisabled = '';
        
        // Shared Quest specific rendering (from sharedQuests collection)
        if(type === 'shared') {
            const isOwner = user && task.ownerUid === user.uid;
            const ownerCompleted = task.ownerCompleted;
            const friendCompleted = task.friendCompleted;
            const otherPlayerUsername = isOwner ? task.friendUsername : task.ownerUsername;
            const allCompleted = ownerCompleted && friendCompleted;
            const myPartCompleted = isOwner ? ownerCompleted : friendCompleted;
            optionsBtnDisabled = allCompleted ? 'disabled' : '';

            li.classList.add('shared-quest');
            if (allCompleted) {
                li.classList.add('all-completed');
            }
            li.dataset.id = task.questId; // Use questId for shared quests

            const selfIdentifier = isOwner ? 'You' : otherPlayerUsername;
            const otherIdentifier = isOwner ? otherPlayerUsername : task.ownerUsername; // Corrected: should be owner's username if current user is friend

            const unshareBtnHTML = isOwner ? `<button class="btn icon-btn unshare-active-btn" aria-label="Unshare Quest" title="Unshare Quest"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line><line x1="1" y1="1" x2="23" y2="23" style="stroke: var(--accent-red); stroke-width: 3px;"></line></svg></button>` : '';
            const abandonBtnHTML = !isOwner ? `<button class="btn icon-btn abandon-quest-btn" aria-label="Abandon Quest" title="Abandon Quest"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg></button>` : '';

            const buttonsHTML = `
                <button class="btn icon-btn timer-clock-btn" aria-label="Set Timer" aria-haspopup="dialog" aria-controls="timer-modal"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><svg class="progress-ring" viewBox="0 0 24 24"><circle class="progress-ring-circle" r="10" cx="12" cy="12"/></svg></button>
                ${unshareBtnHTML}
                ${abandonBtnHTML}
                <button class="btn icon-btn edit-btn" aria-label="Edit Quest" aria-haspopup="dialog" aria-controls="edit-task-modal"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>
                <button class="btn icon-btn delete-btn" aria-label="Delete Quest"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>
            `;

            li.innerHTML = `
                <div class="multi-select-checkbox"></div><div class="completion-indicator"></div>
                <div class="task-content"><span class="task-text">${task.text}</span></div>
                <div class="task-buttons-wrapper">${buttonsHTML}</div><div class="task-actions-container"><button class="btn icon-btn options-btn" aria-label="More options" ${optionsBtnDisabled}><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg></button></div>
                <div class="shared-quest-info">
                    <span class="shared-with-tag">with ${otherPlayerUsername}</span>
                    <div class="shared-status-indicators" title="${selfIdentifier} | ${otherIdentifier}">
                        <div class="status-indicator ${ownerCompleted ? 'completed' : ''}"></div>
                        <div class="status-indicator ${friendCompleted ? 'completed' : ''}"></div>
                    </div>
                </div>`;

            if (myPartCompleted) {
                 li.classList.add('my-part-completed'); // New class for my part completion
            }
            return li;
        }

        // Regular task rendering (from dailyTasks, standaloneMainQuests, generalTaskGroups)
        const isCompleted = task.completedToday || task.pendingDeletion;
        optionsBtnDisabled = isCompleted ? 'disabled' : '';

        let goalHTML = ''; if (type === 'daily' && task.weeklyGoal > 0) { goalHTML = `<div class="weekly-goal-tag" title="Weekly goal"><span>${task.weeklyCompletions}/${task.weeklyGoal}</span></div>`; if (task.weeklyCompletions >= task.weeklyGoal) li.classList.add('weekly-goal-met'); }

        if (task.pendingDeletion) {
            // For pending deletion, show a full-width overlay with just the Undo button.
            li.innerHTML = `<div class="multi-select-checkbox"></div><div class="completion-indicator"></div>
                <div class="task-content"><span class="task-text">${task.text}</span>${goalHTML}</div>
                <div class="task-buttons-wrapper">
                    <button class="btn undo-btn">Undo<div class="undo-timer-bar"></div></button>
                </div>`;
        } else {
            // For normal tasks, show the options button and the actions in the overlay.
            const buttonsHTML = `
                <button class="btn icon-btn timer-clock-btn" aria-label="Set Timer" aria-haspopup="dialog" aria-controls="timer-modal"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><svg class="progress-ring" viewBox="0 0 24 24"><circle class="progress-ring-circle" r="10" cx="12" cy="12"/></svg></button>
                <button class="btn icon-btn share-btn" aria-label="Share Quest" aria-haspopup="dialog" aria-controls="share-quest-modal"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line></svg></button>
                <button class="btn icon-btn edit-btn" aria-label="Edit Quest" aria-haspopup="dialog" aria-controls="edit-task-modal"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>
                <button class="btn icon-btn delete-btn" aria-label="Delete Quest"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>
            `;
            li.innerHTML = `<div class="multi-select-checkbox"></div><div class="completion-indicator"></div><div class="task-content"><span class="task-text">${task.text}</span>${goalHTML}</div>
                <div class="task-buttons-wrapper">${buttonsHTML.trim()}</div><div class="task-actions-container"><button class="btn icon-btn options-btn" aria-label="More options" ${optionsBtnDisabled}><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg></button></div>`;
        }

        if (task.pendingDeletion) li.classList.add('pending-deletion');
        if (task.completedToday) { li.classList.add('daily-completed'); }
        // BUGFIX: A completed or pending-deletion task should never show the 'timer finished' animation,
        // even if the timerFinished flag was not cleared correctly.
        if (task.timerFinished && !task.completedToday && !task.pendingDeletion) {
            li.classList.add('timer-finished');
        }

        // REGRESSION TWEAK: A completed task should never display an active timer.
        // This guard prevents the timer UI from showing even if timer properties were not cleared.        
        if (task.timerStartTime && task.timerDuration && !task.completedToday && !task.pendingDeletion) {
            const elapsed = (Date.now() - task.timerStartTime) / 1000;
            const remaining = Math.max(0, task.timerDuration - elapsed);
            if (remaining > 0) {
                li.classList.add('timer-active');
                // The `resumeTimers` function, called after render, will handle the visual state of the ring.
                // We just need to ensure the class is present.
            }
        }

        // NEW: Handle tasks marked as shared in their original lists.
        // We still render them, but with a different style and disabled interactions.
        if (task.isShared) {
            li.classList.add('is-shared-task');
            const sharedQuest = sharedQuests.find(sq => sq.id === task.sharedQuestId);

            // If the shared quest is not in our list of active/completed quests, it's pending.
            if (!sharedQuest) {
                li.classList.add('pending-share');
                const buttonWrapper = li.querySelector('.task-buttons-wrapper');
                if (buttonWrapper) {
                    buttonWrapper.innerHTML = `
                        <button class="btn icon-btn unshare-btn" data-shared-quest-id="${task.sharedQuestId}" aria-label="Cancel Share" title="Cancel Share"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line><line x1="1" y1="1" x2="23" y2="23" style="stroke: var(--accent-red); stroke-width: 3px;"></line></svg></button>
                    `;
                }
            } else { // Active or completed shared task
                const buttonWrapper = li.querySelector('.task-buttons-wrapper');
                if (buttonWrapper) {
                    buttonWrapper.innerHTML = `<button class="btn view-shared-quest-btn" data-shared-quest-id="${task.sharedQuestId}">View Share</button>`;
                }
            }
        }

        // PERF: Check for overdue status at render time instead of with a setInterval.
        // Only check for non-shared, non-completed tasks.
        if (type !== 'shared' && !task.completedToday && !task.isShared && (Date.now() - task.createdAt) > 86400000) {
            li.classList.add('overdue');
        }

        return li;
    };
    
    const addTask = (text, list, goal) => {
        const common = { id: Date.now().toString(), text, createdAt: Date.now() };
        let newTask, taskType, container;

        if (list === 'daily') {
            newTask = { ...common, completedToday: false, lastCompleted: null, streak: 0, weeklyGoal: goal || 0, weeklyCompletions: 0, weekStartDate: getStartOfWeek(new Date()), isShared: false };
            dailyTasks.push(newTask);
            taskType = 'daily';
            container = dailyTaskListContainer;
        } else if (list === 'standalone') {
            newTask = { ...common, isShared: false };
            standaloneMainQuests.push(newTask);
            taskType = 'standalone';
            container = standaloneTaskListContainer;
        } else {
            const group = generalTaskGroups.find(g => g.id === list);
            if (group) {
                if (!group.tasks) group.tasks = [];
                newTask = { ...common, isShared: false };
                group.tasks.push(newTask);
                taskType = 'group';
                container = document.querySelector(`.task-list-group[data-group-id="${list}"]`);
            }
        }

        if (newTask && container) {
            const taskEl = createTaskElement(newTask, taskType);
            taskEl.classList.add('adding');
            container.appendChild(taskEl);
            taskEl.addEventListener('animationend', () => taskEl.classList.remove('adding'), { once: true });
            
            if (list === 'daily') noDailyTasksMessage.style.display = 'none';
            else noGeneralTasksMessage.style.display = 'none';
        } else {
            renderAllLists();
        }

        saveState(); 
        audioManager.playSound('add');
    };
    const addGroup = (name) => { 
        const newGroup = { id: 'group_' + Date.now(), name, tasks: [], isExpanded: false };
        generalTaskGroups.push(newGroup);

        const groupEl = createGroupElement(newGroup);
        groupEl.classList.add('adding');
        generalTaskListContainer.appendChild(groupEl);
        groupEl.addEventListener('animationend', () => groupEl.classList.remove('adding'), { once: true });
        noGeneralTasksMessage.style.display = 'none';

        saveState(); 
        audioManager.playSound('addGroup'); 
    };
    const editGroup = (id, newName) => {
        const group = generalTaskGroups.find(g => g.id === id);
        if (group) {
            group.name = newName;
            saveState();
            renderAllLists();
            audioManager.playSound('toggle');
        }
    };
    const undoCompleteMainQuest = (id) => {
        if (undoTimeoutMap.has(id)) {
            clearTimeout(undoTimeoutMap.get(id));
            undoTimeoutMap.delete(id);
        }

        const { task, type } = findTaskAndContext(id);
        if (task && task.pendingDeletion) {
            delete task.pendingDeletion;
            addXp(-XP_PER_TASK); // Revert XP gain
            // FIX: Ensure timer properties are cleared when undoing completion
            // to prevent the timer from reappearing.
            delete task.timerFinished;
            delete task.timerStartTime;
            delete task.timerDuration;
            audioManager.playSound('delete'); // Use the 'delete' sound for undo

            // Instead of a full re-render, specifically replace this one element.
            // This is more efficient and guarantees the element with the animation is replaced.
            const oldTaskEl = document.querySelector(`.task-item[data-id="${id}"]`);
            if (oldTaskEl) {
                const newTaskEl = createTaskElement(task, type);
                oldTaskEl.replaceWith(newTaskEl);
            } else {
                // Fallback to full render if the element wasn't found
                renderAllLists();
            }
            saveState(); // Save the state to persist the undo.
        }
    };
    const deleteGroup = (id) => { const name = generalTaskGroups.find(g => g.id === id)?.name || 'this group'; showConfirm(`Delete "${name}"?`, 'All tasks will be deleted.', () => { generalTaskGroups = generalTaskGroups.filter(g => g.id !== id); renderAllLists(); saveState(); audioManager.playSound('delete'); }); };
    const findTaskAndContext = (id) => {
        let task = dailyTasks.find(t => t && t.id === id); if (task) return { task, list: dailyTasks, type: 'daily' };
        task = standaloneMainQuests.find(t => t && t.id === id); if(task) return { task, list: standaloneMainQuests, type: 'standalone'};
        for (const g of generalTaskGroups) { if (g && g.tasks) { const i = g.tasks.findIndex(t => t && t.id === id); if (i !== -1) return { task: g.tasks[i], list: g.tasks, group: g, type: 'group' }; } } 
        task = sharedQuests.find(t => t && t.questId === id); if (task) return { task, list: sharedQuests, type: 'shared' };
        const group = sharedGroups.find(g => g && g.id === id); if (group) return { group, type: 'shared-group' };
        return {};
    };
    const deleteTask = (id) => { 
        stopTimer(id, false); 
        const {task, list, type} = findTaskAndContext(id); 
        if (!task || !list) return;

        // Prevent deletion of shared tasks from original lists, with a special case for orphans.
        if (task.isShared && type !== 'shared') {
            const sharedQuestData = questsMap.get(task.sharedQuestId);
            
            // An orphan is a quest where the share doc doesn't exist,
            // OR the share doc exists but the other participant is no longer a friend.
            const otherParticipant = sharedQuestData ? sharedQuestData.participants.find(p => p !== user.uid) : null;
            const isOrphan = !sharedQuestData || (otherParticipant && !confirmedFriendUIDs.includes(otherParticipant));

            if (isOrphan) {
                showConfirm(
                    "Clean Up Orphaned Quest?", 
                    "This shared quest seems to be orphaned (it may have been removed by your friend or the share no longer exists). Would you like to convert it back to a normal quest?", 
                    () => {
                        revertSharedQuest(task.id);
                        // Also try to delete the Firestore doc if it exists, just in case.
                        if (sharedQuestData) {
                            deleteDoc(doc(db, "sharedQuests", sharedQuestData.id)).catch(err => console.warn("Orphaned share doc cleanup failed:", err));
                        }
                    }
                );
            } else {
                showConfirm("Shared Quest", "This quest has been shared. It cannot be deleted from here. You can only delete it from the Shared Quests section once it's completed by both participants.", () => {});
            }
            return;
        }

        // NEW: Only allow deletion of shared quests from the shared list if both completed.
        if (type === 'shared') {
            const otherParticipantUid = user.uid === task.ownerUid ? task.friendUid : task.ownerUid;
            const isFriend = confirmedFriendUIDs.includes(otherParticipantUid);

            // Block deletion ONLY if the quest is active AND the other user is still a friend.
            if (task.status === 'active' && isFriend) {
                showConfirm("Cannot Delete Shared Quest", "This shared quest is still active. It can only be deleted once both participants have completed it.", () => {});
                return;
            }
            // For all other cases (pending, completed, rejected, or active but friend removed), allow deletion.
            const confirmText = isFriend ? "This will delete the quest for all participants." : "This will remove the orphaned shared quest.";
            showConfirm("Delete Shared Quest?", confirmText, async () => {
                try {
                    // When deleting an orphaned quest, we also need to revert the original task if we are the owner.
                    if (!isFriend && task.ownerUid === user.uid) {
                        revertSharedQuest(task.originalTaskId);
                    }
                    const questDocRef = doc(db, "sharedQuests", id);
                    await deleteDoc(questDocRef);
                    // The onSnapshot listener will handle UI updates
                    audioManager.playSound('delete');
                } catch (error) {
                    console.error("Error deleting shared quest:", getCoolErrorMessage(error));
                    showConfirm("Error", "Failed to delete shared quest. Please try again later.", () => {});
                }
            });
        } else {
            showConfirm(`Delete "${task.text}"?`, 'This action cannot be undone.', () => {
                const i = list.findIndex(t => t.id === id);
                if (i > -1) {
                    list.splice(i, 1);
                    // PERF: Instead of re-rendering, find and remove the specific element.
                    const taskEl = document.querySelector(`.task-item[data-id="${id}"]`);
                    if (taskEl) {
                        taskEl.classList.add('removing');
                        taskEl.addEventListener('animationend', () => taskEl.remove(), { once: true });
                    } else {
                        renderAllLists(); // Fallback if element not found
                    }
                    saveState();
                    audioManager.playSound('delete');
                }
            });
        }
    };
    const completeTask = (id) => {
        stopTimer(id, false);
        const { task, type } = findTaskAndContext(id);
        if (!task) return;

        // NEW: Prevent completion of shared tasks from original lists
        if (task.isShared && type !== 'shared') {
            showConfirm("Shared Quest", "This quest has been shared. Manage its completion in the Shared Quests section.", () => {});
            return;
        }

        if (type === 'shared') {
            completeSharedQuestPart(task);
            return;
        }

        // For main quests, if it's already pending deletion, do nothing.
        if ((type === 'standalone' || type === 'group') && task.pendingDeletion) {
            return;
        }
        // For daily quests, if it's already completed, do nothing.
        if (type === 'daily' && task.completedToday) {
            return;
        }

        // Explicitly delete all timer properties on completion.
        // This is the most reliable way to ensure that a completed task, if un-completed,
        // does not retain any 'finished' or 'running' timer state. This provides an
        // extra layer of safety on top of the stopTimer() call.
        delete task.timerFinished;
        delete task.timerStartTime;
        delete task.timerDuration;

        addXp(XP_PER_TASK);
        audioManager.playSound('complete');
        if (type === 'daily') {
            task.completedToday = true;
            task.lastCompleted = new Date().toDateString();
            if (task.weeklyGoal > 0) {
                const now = new Date();
                if (task.weekStartDate < getStartOfWeek(now)) {
                    task.weekStartDate = getStartOfWeek(now);
                    task.weeklyCompletions = 1;
                } else {
                    task.weeklyCompletions = (task.weeklyCompletions || 0) + 1;
                }
            }
        } else {
            task.pendingDeletion = true;

            if (undoTimeoutMap.has(id)) {
                clearTimeout(undoTimeoutMap.get(id));
            }

            const timeoutId = setTimeout(() => {
                const { list, group } = findTaskAndContext(id);
                if (list) {
                    const i = list.findIndex(t => t.id === id);
                    if (i > -1) list.splice(i, 1);
                }
                if (group && (!group.tasks || group.tasks.length === 0)) {
                    const i = generalTaskGroups.findIndex(g => g.id === group.id);
                    if (i > -1) generalTaskGroups.splice(i, 1);
                }
                undoTimeoutMap.delete(id);
                saveState();
                renderAllLists();
            }, 5000); // 5 seconds to undo

            undoTimeoutMap.set(id, timeoutId);
        }
        if (type === 'daily') saveState();

        // Replace the element directly to stop animations and update state instantly.
        const oldTaskEl = document.querySelector(`.task-item[data-id="${id}"]`);
        if (oldTaskEl) {
            const newTaskEl = createTaskElement(task, type);
            oldTaskEl.replaceWith(newTaskEl);
            if (type !== 'daily') { // Main quest completion
                createConfetti(newTaskEl);
            }
        } else {
            renderAllLists(); // Fallback if element not found
        }

        const { allDailiesDone, allTasksDone } = checkAllTasksCompleted();
        if (allTasksDone) createFullScreenConfetti(true);
        else if (allDailiesDone) createFullScreenConfetti(false);
    };
    const uncompleteDailyTask = (id) => {
        const { task, type } = findTaskAndContext(id);
        if (!task) return;

        // NEW: Prevent uncompletion of shared tasks from original lists
        if (task.isShared && type !== 'shared') {
            showConfirm("Shared Quest", "This quest has been shared. Manage its completion in the Shared Quests section.", () => {});
            return;
        }
            
        if(type === 'shared') { // Un-complete shared task part
            completeSharedQuestPart(task, true); // `true` to un-complete
            return;
        }

        if (task.completedToday) {
            task.completedToday = false;
            delete task.timerFinished;
            // FIX: Ensure timer properties are cleared when un-completing a task
            // to prevent it from resuming on page reload.
            delete task.timerStartTime;
            delete task.timerDuration;
            if (task.weeklyGoal > 0 && task.lastCompleted === new Date().toDateString()) {
                task.weeklyCompletions = Math.max(0, (task.weeklyCompletions || 0) - 1);
            }
            addXp(-XP_PER_TASK);
            audioManager.playSound('delete');
            saveState();

            // Replace the element directly to stop animations and update state instantly.
            const oldTaskEl = document.querySelector(`.task-item[data-id="${id}"]`);
            if (oldTaskEl) {
                const newTaskEl = createTaskElement(task, type);
                oldTaskEl.replaceWith(newTaskEl);
            } else {
                renderAllLists(); // Fallback if element not found
            }
        }
    };
    const editTask = async (id, text, goal) => {
        const { task, type } = findTaskAndContext(id);
        if (task) {
            if (type === 'shared') {
                if (user.uid !== task.ownerUid) {
                    showConfirm("Cannot Edit", "Only the owner of a shared quest can edit it.", () => {});
                    return;
                }
                try {
                    const sharedQuestRef = doc(db, "sharedQuests", id);
                    await updateDoc(sharedQuestRef, { text: text });
                    audioManager.playSound('toggle');
                } catch (error) {
                    console.error("Error updating shared quest:", getCoolErrorMessage(error));
                    showConfirm("Error", "Could not update shared quest.", () => {});
                }
                return;
            }
            // NEW: Prevent editing a task that's been shared from its original list
            if (task.isShared) { 
                showConfirm("Cannot Edit", "This quest has been shared. It cannot be edited from here.", () => {});
                return;
            }
            task.text = text;
            if (type === 'daily') task.weeklyGoal = goal;
            saveState();
            renderAllLists();
        }
    };
    const openEditSharedTaskModal = (groupId, taskId) => {
        const group = sharedGroups.find(g => g.id === groupId);
        if (!group) return;
        const task = group.tasks.find(t => t.id === taskId);
        if (!task) return;

        // Repurpose the existing edit task modal
        currentEditingTaskId = JSON.stringify({ groupId, taskId }); // Store both IDs
        editTaskIdInput.value = currentEditingTaskId; // Hidden input
        editTaskInput.value = task.text;
        editTaskModal.querySelector('#edit-task-modal-title').textContent = 'Edit Shared Task';
        editWeeklyGoalContainer.style.display = 'none'; // No weekly goals for shared tasks
        openModal(editTaskModal);
        focusOnDesktop(editTaskInput);
    };
    const addTaskToSharedGroup = async (groupId, text) => {
        const groupRef = doc(db, "sharedGroups", groupId);
        const newTask = {
            id: Date.now().toString(),
            text: text,
            ownerCompleted: false,
            friendCompleted: false
        };
        try {
            await updateDoc(groupRef, {
                tasks: arrayUnion(newTask)
            });
            audioManager.playSound('add');
        } catch (error) {
            console.error("Error adding task to shared group:", getCoolErrorMessage(error));
            showConfirm("Error", "Could not add task to the shared group.", () => {});
        }
    };
    const editSharedGroupName = async (groupId, newName) => {
        const groupRef = doc(db, "sharedGroups", groupId);
        try {
            await updateDoc(groupRef, { name: newName });
            audioManager.playSound('toggle');
        } catch (error) {
            console.error("Error editing shared group name:", getCoolErrorMessage(error));
            showConfirm("Error", "Could not edit group name.", () => {});
        }
    };
    const deleteSharedTask = async (groupId, taskId) => {
        const groupRef = doc(db, "sharedGroups", groupId);
        showConfirm("Delete Task?", "This will delete the task for both you and your friend.", async () => {
            try {
                const groupDoc = await getDoc(groupRef);
                if (groupDoc.exists()) {
                    const groupData = groupDoc.data();
                    const taskToRemove = groupData.tasks.find(t => t.id === taskId);
                    if (taskToRemove) {
                        await updateDoc(groupRef, {
                            tasks: arrayRemove(taskToRemove)
                        });
                        audioManager.playSound('delete');
                    }
                }
            } catch (error) {
                console.error("Error deleting shared task:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not delete task.", () => {});
            }
        });
    };
    const editSharedTask = async (groupId, taskId, newText) => {
        const groupRef = doc(db, "sharedGroups", groupId);
        try {
            const groupDoc = await getDoc(groupRef);
            if (groupDoc.exists()) {
                const tasks = groupDoc.data().tasks;
                const taskIndex = tasks.findIndex(t => t.id === taskId);
                if (taskIndex > -1) {
                    tasks[taskIndex].text = newText;
                    await updateDoc(groupRef, { tasks: tasks });
                    audioManager.playSound('toggle');
                }
            }
        } catch (error) {
            console.error("Error editing shared task:", getCoolErrorMessage(error));
            showConfirm("Error", "Could not edit task.", () => {});
        }
    };
    const completeSharedGroupTask = async (groupId, taskId, uncompleting = false) => {
        const groupRef = doc(db, "sharedGroups", groupId);
        try {
            const groupDoc = await getDoc(groupRef);
            if (!groupDoc.exists()) return;
    
            const groupData = groupDoc.data();
            const taskIndex = groupData.tasks.findIndex(t => t.id === taskId);
            if (taskIndex === -1) return;
    
            const taskToUpdate = groupData.tasks[taskIndex];
            const isOwner = user.uid === groupData.ownerUid;
    
            if (isOwner) taskToUpdate.ownerCompleted = !uncompleting;
            else taskToUpdate.friendCompleted = !uncompleting;
    
            const allTasksCompleted = groupData.tasks.every(t => t.ownerCompleted && t.friendCompleted);
            
            const updatePayload = { tasks: groupData.tasks };
            if (allTasksCompleted) {
                updatePayload.status = 'completed';
            }
    
            await updateDoc(groupRef, updatePayload);
    
            if (!uncompleting) {
                audioManager.playSound('complete');
                addXp(XP_PER_TASK / 2);
            } else {
                audioManager.playSound('delete');
                addXp(-(XP_PER_TASK / 2));
            }
        } catch (error) {
            console.error("Error completing shared group task:", getCoolErrorMessage(error));
        }
    };
    const revertSharedQuest = (originalTaskId) => {
        if (!originalTaskId) return;
        const { task } = findTaskAndContext(originalTaskId);
        if (task?.isShared) { // Explicitly check for shared status
            task.isShared = false;
            delete task.sharedQuestId;
            saveState();
            renderAllLists();
            audioManager.playSound('delete'); // Play sound on successful revert
        }
    };

    const unshareQuest = async (questId) => {
        const { task: sharedQuest, type: taskType } = findTaskAndContext(questId);
        if (!sharedQuest || taskType !== 'shared') return;

        if (user.uid !== sharedQuest.ownerUid) {
            showConfirm("Cannot Unshare", "Only the owner can unshare a quest.", () => {});
            return;
        }

        showConfirm("Unshare Quest?", "This will convert it back to a normal quest for you and remove it for your friend. Are you sure?", async () => {
            try {
                // Instead of deleting directly, update the status.
                // The owner's own listener will see this change and perform the deletion and local state reversion,
                // centralizing the cleanup logic into a flow that is known to work.
                await updateDoc(doc(db, "sharedQuests", questId), { status: 'unshared' });
            } catch (error) {
                console.error("Error unsharing quest:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not unshare the quest.", () => {});
            }
        });
    };

    const abandonQuest = async (questId) => {
        const { task: sharedQuest } = findTaskAndContext(questId);
        if (!sharedQuest || (user && sharedQuest.ownerUid === user.uid)) return; // Only friend can abandon

        showConfirm("Abandon Quest?", "This will remove the quest from your list and the owner will be notified. Are you sure?", async () => {
            try {
                const sharedQuestRef = doc(db, "sharedQuests", questId);
                await updateDoc(sharedQuestRef, { status: 'abandoned' });
                audioManager.playSound('delete');
            } catch (error) {
                console.error("Error abandoning quest:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not abandon the quest.", () => {});
            }
        });
    };
    const cancelShare = async (originalTaskId) => {
        // The parameter is the ID of the sharedQuest document itself.
        const sharedQuestId = originalTaskId;
        if (!sharedQuestId) return;
        showConfirm("Cancel Share?", "This will cancel the pending share request.", async () => {
            try {
                const sharedQuestRef = doc(db, "sharedQuests", sharedQuestId);
                const sharedQuestSnap = await getDoc(sharedQuestRef);

                if (!sharedQuestSnap.exists()) {
                    console.log("Share to cancel not found, it was likely already handled.");
                    // Revert the local task just in case the listener is slow or failed
                    let originalTask;
                    [...dailyTasks, ...standaloneMainQuests, ...generalTaskGroups.flatMap(g => g.tasks || [])].forEach(t => {
                        if (t && t.sharedQuestId === sharedQuestId) originalTask = t;
                    });
                    if (originalTask) revertSharedQuest(originalTask.id);
                    return;
                }

                const questData = sharedQuestSnap.data();

                if (questData.ownerUid !== user.uid) {
                    showConfirm("Error", "You are not the owner of this share.", () => {});
                    return;
                }

                if (questData.status !== 'pending') {
                    console.log("Attempted to cancel a share that was no longer pending. UI will update shortly.");
                    return;
                }

                // FIX: Instead of deleting directly (which causes a permission error), we update the status.
                // The owner's own Firestore listener will detect the 'unshared' status,
                // revert the local task, and then delete the document. This centralizes the cleanup logic
                // and aligns it with the pattern used for abandoning/unsharing active quests.
                await updateDoc(sharedQuestRef, { status: 'unshared' });
                
            } catch (error) {
                console.error("Error cancelling share:", getCoolErrorMessage(error));
                showConfirm("Error", error.message || "Could not cancel the share. Please try again.", () => {});
            }
        });
    };
    const cancelSharedGroup = async (sharedGroupId) => {
        if (!sharedGroupId) return;

        showConfirm("Cancel Share?", "This will cancel the pending group share request.", async () => {
            try {
                const sharedGroupRef = doc(db, "sharedGroups", sharedGroupId);
                const sharedGroupSnap = await getDoc(sharedGroupRef);

                if (!sharedGroupSnap.exists()) {
                    console.log("Group share to cancel not found, it was likely already handled.");
                    const originalGroup = generalTaskGroups.find(g => g.sharedGroupId === sharedGroupId);
                    if (originalGroup) {
                        delete originalGroup.isShared;
                        delete originalGroup.sharedGroupId;
                        renderAllLists();
                        saveState();
                    }
                    return;
                }

                const groupData = sharedGroupSnap.data();

                if (groupData.ownerUid !== user.uid) {
                    showConfirm("Error", "You are not the owner of this group share.", () => {});
                    return;
                }

                if (groupData.status !== 'pending') {
                    console.log("Attempted to cancel a group share that was no longer pending.");
                    return;
                }

                // Owner cancels by deleting the pending request.
                await deleteDoc(sharedGroupRef);

                // Revert the original group in local state for responsiveness.
                const originalGroup = generalTaskGroups.find(g => g.id === groupData.originalGroupId);
                if (originalGroup) {
                    delete originalGroup.isShared;
                    delete originalGroup.sharedGroupId;
                }
                
                audioManager.playSound('delete');
                renderAllLists();
                saveState(); // Save the reverted state
            } catch (error) {
                console.error("Error cancelling group share:", getCoolErrorMessage(error));
                showConfirm("Error", error.message || "Could not cancel the share. Please try again.", () => {});
            }
        });
    };

    const finishTimer = (id) => {
        audioManager.playSound('timerUp');
        activeTimers.delete(id); // Ensure it's removed from the active map.

        const { task } = findTaskAndContext(id);
        if (task) {
            // BUGFIX: If the task has already been completed, do not mark its timer as finished.
            // This prevents a race condition where a timer finishes after the task is marked complete,
            // which would cause the 'shaking' animation to reappear on un-completion.
            if (task.completedToday || task.pendingDeletion) {
                // Just ensure the timer properties are gone and exit.
                delete task.timerStartTime;
                delete task.timerDuration;
                saveState();
                return;
            }

            task.timerFinished = true;
            delete task.timerStartTime;
            delete task.timerDuration;
            renderAllLists(); // Re-render to show the 'finished' state.
            saveState();
        }
    };

    const startTimer = (id, mins) => {
        stopTimer(id, false);
        const { task, type } = findTaskAndContext(id);
        if (!task) return;

        if (type !== 'shared' && task.isShared) {
            showConfirm("Cannot Set Timer", "This quest is pending a share and cannot have a timer.", () => {});
            return;
        }

        task.timerStartTime = Date.now();
        task.timerDuration = mins * 60;
        delete task.timerFinished;
        if (type !== 'shared') saveState();

        renderAllLists(); // Re-render to apply 'timer-active' class and allow resumeTimers to pick it up.
    };

    const stopTimer = (id, shouldRender = true) => {
        if (activeTimers.has(id)) {
            clearTimeout(activeTimers.get(id));
            activeTimers.delete(id);
        }

        const { task } = findTaskAndContext(id);
        if (task) {
            delete task.timerStartTime;
            delete task.timerDuration;
            delete task.timerFinished;

            // Also reset the ring's CSS when a timer is stopped.
            const taskEl = document.querySelector(`.task-item[data-id="${id}"]`);
            if (taskEl) {
            // By removing the class, we disable all timer-related CSS rules,
            // which is the most robust way to stop the animation.
            taskEl.classList.remove('timer-active');
                const ringEl = taskEl.querySelector('.progress-ring-circle');
                if (ringEl) {
                // Resetting the transition and offset is good practice to prevent flashes.
                    ringEl.style.transitionDuration = '0s';
                    ringEl.style.strokeDashoffset = 0;
                }
            }

            if (shouldRender) {
                renderAllLists();
                saveState();
            }
        }
    };

    const resumeTimers = () => {
        // Clear any previously running timeouts that haven't been explicitly stopped.
        activeTimers.forEach(timeoutId => clearTimeout(timeoutId));
        activeTimers.clear();

        let needsSaveAndRender = false;
        const allTasks = [...dailyTasks, ...standaloneMainQuests, ...generalTaskGroups.flatMap(g => g.tasks || [])];
        
        allTasks.forEach(t => {
            const isCompleted = t.completedToday || t.pendingDeletion;
            if (t && t.timerStartTime && t.timerDuration && !t.isShared && !isCompleted) {
                const elapsed = (Date.now() - t.timerStartTime) / 1000;
                const remaining = Math.max(0, t.timerDuration - elapsed);

                if (remaining > 0) {
                    const taskEl = document.querySelector(`.task-item[data-id="${t.id}"]`);
                    if (taskEl) {
                        const ringEl = taskEl.querySelector('.progress-ring-circle');
                        if (ringEl) {
                            const r = 10; // from CSS
                            const c = r * 2 * Math.PI;
                            const startOffset = (elapsed / t.timerDuration) * c;

                            // Set initial state without transition
                            ringEl.style.transitionDuration = '0s';
                            ringEl.style.strokeDashoffset = startOffset;
                            
                            // Force reflow to apply the initial state
                            ringEl.getBoundingClientRect(); 

                            // Apply transition and set final state
                            ringEl.style.transitionDuration = `${remaining}s`;
                            ringEl.style.strokeDashoffset = c;

                            // Set a timeout to call finishTimer when it's done
                            const timeoutId = setTimeout(() => finishTimer(t.id), remaining * 1000);
                            activeTimers.set(t.id, timeoutId);
                        }
                    }
                } else {
                    // Timer has finished while the app was closed or tab was inactive
                    if (!t.timerFinished) {
                        t.timerFinished = true;
                        delete t.timerStartTime;
                        delete t.timerDuration;
                        needsSaveAndRender = true;
                    }
                }
            }
        });

        if (needsSaveAndRender) {
            saveState();
            renderAllLists();
        }
    };

    function toggleTaskActions(element) {
        if (element.classList.contains('timer-active')) {
            return;
        }

        const wasVisible = element.classList.contains('actions-visible');

        // Always hide any currently active actions first.
        // This handles clicking a new item or clicking the same item again.
        hideActiveTaskActions();

        // If we didn't just close the actions on the clicked element, open them.
        if (!wasVisible) {
            const optionsBtn = element.querySelector('.options-btn');
            element.classList.add('actions-visible');
            if (optionsBtn) optionsBtn.classList.add('is-active-trigger');
            activeMobileActionsItem = element;
        }
    }

    document.querySelector('.quests-layout').addEventListener('click', (e) => {
        const taskItem = e.target.closest('.task-item');
        const groupHeader = e.target.closest('.main-quest-group-header');
        const groupElement = groupHeader ? groupHeader.parentElement : null;
        const clickableItem = taskItem || groupElement;
        const selectableItem = taskItem || groupHeader;
        const selectableParent = groupHeader ? groupHeader.parentElement : null;
        const idForSelection = selectableItem ? (selectableItem.dataset.id || (selectableParent ? selectableParent.dataset.groupId : null)) : null;


        if (clickableItem) {
            // Use the most specific ID available on the clicked item.
            const itemId = clickableItem.dataset.id || clickableItem.dataset.groupId || clickableItem.dataset.sharedGroupId;
            if (itemId) {
                const now = Date.now();
                const lastClick = lastClickTimes.get(itemId) || 0;
                if (now - lastClick < CLICK_DEBOUNCE_TIME) {
                    e.preventDefault();
                    e.stopPropagation();
                    return; // Debounce this click to prevent double-click issues.
                }
                lastClickTimes.set(itemId, now);
            }
        }

        if (isMultiSelectModeActive) {
            if (!selectableItem) return;

            // Don't select if clicking on an action button inside
            if (e.target.closest('.task-buttons-wrapper, .group-actions, .options-btn')) {
                return;
            }

            e.preventDefault();
            e.stopPropagation();

            if (!idForSelection) return;

            if (selectedQuestIds.has(idForSelection)) {
                selectedQuestIds.delete(idForSelection);
                (taskItem || groupElement).classList.remove('multi-select-selected');
            } else {
                selectedQuestIds.add(idForSelection);
                (taskItem || groupElement).classList.add('multi-select-selected');
            }
            updateBatchActionsUI();
            return; // Stop further processing
        }
        
        if (groupHeader) { 
            const parentEl = groupHeader.parentElement;

            // NEW: Handle shared groups first
            if (parentEl.classList.contains('shared-quest-group')) {
                const sharedGroupId = parentEl.dataset.sharedGroupId;
                const group = sharedGroups.find(g => g.id === sharedGroupId);

                if (e.target.closest('.options-btn')) {
                    shiftHoverItem = null; // A click on options button takes precedence
                    toggleTaskActions(groupHeader);
                    return;
                }

                if (e.target.closest('.group-actions')) {
                    const isEditClick = e.target.closest('.edit-group-btn');
                    const isAddClick = e.target.closest('.add-task-to-group-btn');
                    const unshareBtn = e.target.closest('.unshare-group-btn');
                    const abandonBtn = e.target.closest('.abandon-group-btn');

                    if (isEditClick) {
                        currentEditingGroupId = sharedGroupId;
                        addGroupModal.querySelector('h2').textContent = 'Edit Group Name';
                        addGroupModal.querySelector('.modal-submit-btn').textContent = 'Save';
                        newGroupInput.value = group.name;
                        openModal(addGroupModal);
                        focusOnDesktop(newGroupInput);
                        return;
                    }
                    if (isAddClick) {
                        currentListToAdd = `shared-group-${sharedGroupId}`;
                        weeklyGoalContainer.style.display = 'none';
                        addTaskModalTitle.textContent = `Add to "${group.name}"`;
                        openModal(addTaskModal);
                        focusOnDesktop(newTaskInput);
                        return;
                    }
                    if (unshareBtn) { unshareSharedGroup(sharedGroupId); return; }
                    if (abandonBtn) { abandonSharedGroup(sharedGroupId); return; }
                    return; // Click was inside the actions overlay
                }
                
                // Click on header body to expand/collapse
                if (group) {
                    group.isExpanded = !group.isExpanded;
                    parentEl.classList.toggle('expanded', group.isExpanded);
                }
                return;
            }

            // It's a normal group header.
            const groupId = parentEl.dataset.groupId;
            const g = generalTaskGroups.find(g => g.id === groupId);

            if (e.target.closest('.options-btn')) {
                shiftHoverItem = null; // A click on options button takes precedence
                toggleTaskActions(groupHeader);
                return;
            }

            if (e.target.closest('.group-actions') || e.target.closest('.cancel-share-group-btn') || e.target.closest('.cleanup-orphan-group-btn')) {
                const isAddClick = e.target.closest('.add-task-to-group-btn');
                const isDeleteClick = e.target.closest('.delete-group-btn');
                const isEditClick = e.target.closest('.edit-group-btn');
                const isShareClick = e.target.closest('.share-group-btn');
                const isCancelShareClick = e.target.closest('.cancel-share-group-btn');
                const isCleanupClick = e.target.closest('.cleanup-orphan-group-btn');

                if (isAddClick) {
                    currentListToAdd = groupId; 
                    weeklyGoalContainer.style.display = 'none'; 
                    addTaskModalTitle.textContent = `Add to "${g.name}"`; 
                    openModal(addTaskModal); 
                    focusOnDesktop(newTaskInput);
                    return;
                } 
                if (isDeleteClick) {
                    deleteGroup(groupId);
                    return;
                }
                if (isEditClick) {
                    if (g) {
                        currentEditingGroupId = groupId;
                        addGroupModal.querySelector('h2').textContent = 'Edit Group Name';
                        addGroupModal.querySelector('.modal-submit-btn').textContent = 'Save';
                        newGroupInput.value = g.name;
                        openModal(addGroupModal);
                        focusOnDesktop(newGroupInput);
                    }
                    return;
                }
                if (isShareClick) {
                    if (!user) {
                        showConfirm("Login Required", "You must be logged in to share groups.", () => {
                            closeModal(shareGroupModal);
                            openModal(accountModal);
                        });
                        return;
                    }
                    openShareGroupModal(groupId);
                    return;
                }
                if (isCancelShareClick) {
                    const sharedGroupId = isCancelShareClick.dataset.sharedGroupId;
                    cancelSharedGroup(sharedGroupId);
                    return;
                }
                if (isCleanupClick) {
                    const groupId = isCleanupClick.dataset.groupId;
                    const sharedGroupId = isCleanupClick.dataset.sharedGroupId;
                    showConfirm(
                        "Clean Up Orphaned Group?",
                        "This will convert the group back to a normal, editable group in your list.",
                        () => {
                            // Revert local state
                            const localGroup = generalTaskGroups.find(g => g.id === groupId);
                            if (localGroup) {
                                delete localGroup.isShared;
                                delete localGroup.sharedGroupId;
                                saveState();
                                renderAllLists();
                            }
                            // Attempt to delete the remote doc just in case
                            if (sharedGroupId) {
                                deleteDoc(doc(db, "sharedGroups", sharedGroupId)).catch(err => console.warn("Orphaned group doc cleanup failed:", err));
                            }
                        }
                    );
                    return;
                }
                // If the click was on the overlay but not on a specific button, close the actions.
                hideActiveTaskActions();
                return; // Event handled, don't fall through to expand/collapse
            }
            
            // Click on header body to expand/collapse
            if (g) {
                g.isExpanded = !g.isExpanded; 
                groupHeader.parentElement.classList.toggle('expanded', g.isExpanded);
            }
            return; 
        }

        if (taskItem) {
            const id = taskItem.dataset.id;
            const { task, type } = findTaskAndContext(id);

            // Helper to determine if the task is completed by the current user
            const isMyPartCompleted = () => {
                if (!task) return false;
                // This handles standalone shared quests
                if (type === 'shared') {
                    const isOwner = user && task.ownerUid === user.uid;
                    return isOwner ? task.ownerCompleted : task.friendCompleted;
                }
                // This handles daily quests
                return task.completedToday;
            };
            
            // NEW: Handle undo button click
            if (e.target.closest('.undo-btn')) {
                undoCompleteMainQuest(id);
                return;
            }

            // --- Case 1: Task is inside a Shared Group ---
            const sharedGroupId = taskItem.dataset.sharedGroupId;
            if (sharedGroupId) {
                const group = sharedGroups.find(g => g.id === sharedGroupId);
                if (!group) return;
                const sharedTask = group.tasks.find(t => t.id === id);
                if (!sharedTask) return;
                
                if (e.target.closest('.options-btn')) {
                    shiftHoverItem = null; // A click on options button takes precedence
                    toggleTaskActions(taskItem);
                } else if (e.target.closest('.task-buttons-wrapper')) {
                    // Clicks inside the actions menu
                } else if (e.target.closest('.delete-btn')) {
                    deleteSharedTask(sharedGroupId, id);
                } else if (e.target.closest('.edit-btn')) {
                    openEditSharedTaskModal(sharedGroupId, id);
                } else if (e.target.closest('.timer-clock-btn')) {
                    showConfirm("Not Implemented", "Timers for shared tasks are not yet supported.", () => {});
                } else if (!e.target.closest('button')) {
                    // Click on body to complete
                    const isOwner = user.uid === group.ownerUid;
                    const uncompleting = isOwner ? sharedTask.ownerCompleted : sharedTask.friendCompleted;
                    completeSharedGroupTask(sharedGroupId, id, uncompleting);
                }
                return;
            }
            
            // --- Case 2: Task is a normal task (not in a shared group) ---
            if (e.target.closest('.options-btn')) {
                shiftHoverItem = null; // A click on options button takes precedence
                toggleTaskActions(taskItem);
                return;
            }

            // Check for clicks inside the actions overlay
            if (e.target.closest('.task-buttons-wrapper')) {
                currentEditingTaskId = id; // For normal tasks
                if (e.target.closest('.delete-btn')) deleteTask(id);
                else if (e.target.closest('.view-shared-quest-btn')) {
                    const viewBtn = e.target.closest('.view-shared-quest-btn');
                    const sharedQuestId = viewBtn.dataset.sharedQuestId;
                    const sharedQuestEl = document.querySelector(`.task-item.shared-quest[data-id="${sharedQuestId}"]`);
                    if (sharedQuestEl) {
                        const isMobile = window.matchMedia("(max-width: 1023px)").matches;
                        const dailySection = document.querySelector('.task-group[data-section="daily"]');
                        let sectionWasSwitched = false;

                        // If on mobile and the daily section is not visible, switch to it.
                        if (isMobile && dailySection && !dailySection.classList.contains('mobile-visible')) {
                            sectionWasSwitched = true;
                            
                            // Switch visible section
                            document.querySelectorAll('.task-group').forEach(group => {
                                group.classList.toggle('mobile-visible', group.dataset.section === 'daily');
                            });

                            // Switch active nav button
                            mobileNav.querySelectorAll('.mobile-nav-btn').forEach(btn => {
                                btn.classList.toggle('active', btn.dataset.section === 'daily');
                            });
                            
                            lastSection = 'daily';
                            audioManager.playSound('toggle');
                        }

                        const scrollAndAnimate = () => {
                            sharedQuestEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            sharedQuestEl.classList.add('friend-completed-pulse');
                            sharedQuestEl.addEventListener('animationend', () => sharedQuestEl.classList.remove('friend-completed-pulse'), { once: true });
                        };

                        if (sectionWasSwitched) {
                            // Use a small timeout to allow the DOM to update if the section was hidden
                            setTimeout(scrollAndAnimate, 50);
                        } else {
                            scrollAndAnimate();
                        }
                    }
                    if (taskItem.classList.contains('actions-visible')) {
                        toggleTaskActions(taskItem);
                    }
                    return;
                }
                else if (e.target.closest('.view-shared-group-btn')) {
                    const viewBtn = e.target.closest('.view-shared-group-btn');
                    const sGroupId = viewBtn.dataset.sharedGroupId;
                    const sharedGroupEl = document.querySelector(`.shared-quest-group[data-shared-group-id="${sGroupId}"]`);
                    if (sharedGroupEl) {
                        const scrollAndAnimate = () => {
                            sharedGroupEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            sharedGroupEl.classList.add('friend-completed-pulse');
                            sharedGroupEl.addEventListener('animationend', () => sharedGroupEl.classList.remove('friend-completed-pulse'), { once: true });
                        };
                        // This assumes the shared items are always visible, which they are.
                        scrollAndAnimate();
                    }
                    // Close the actions overlay on the original group item
                    const groupHeader = e.target.closest('.main-quest-group-header');
                    if (groupHeader && groupHeader.classList.contains('actions-visible')) {
                        toggleTaskActions(taskItem);
                    }
                    return;
                }
                else if (e.target.closest('.unshare-btn')) { // For pending shares
                    const unshareBtn = e.target.closest('.unshare-btn');
                    const sharedQuestId = unshareBtn.dataset.sharedQuestId;
                    cancelShare(sharedQuestId); // This was the call with the permission error
                }
                else if (e.target.closest('.share-btn')) {
                    if (task && task.isShared) {
                        showConfirm("Shared Quest", "This quest has already been shared.", () => {});
                        return;
                    }
                    openShareModal(id);
                }
                else if (e.target.closest('.unshare-active-btn')) {
                    unshareQuest(id);
                }
                else if (e.target.closest('.abandon-quest-btn')) {
                    abandonQuest(id);
                }
                else if (e.target.closest('.timer-clock-btn')) {
                    if (task && task.timerStartTime) openModal(timerMenuModal); else openModal(timerModal);
                }
                else if (e.target.closest('.edit-btn')) {
                    if (task) {
                        const isSharedFromList = type === 'shared';
                        const isSharedPlaceholder = !isSharedFromList && task.isShared;

                        if (isSharedPlaceholder) {
                            showConfirm("Cannot Edit", "This quest has been shared. It cannot be edited from here.", () => {});
                            return;
                        }

                        if (isSharedFromList && user.uid !== task.ownerUid) {
                            showConfirm("Cannot Edit", "Only the owner of a shared quest can edit it.", () => {});
                            return;
                        }

                        editTaskIdInput.value = id;
                        editTaskInput.value = task.text;
                        editTaskModal.querySelector('#edit-task-modal-title').textContent = isSharedFromList ? 'Edit Shared Quest' : ((type === 'daily') ? 'Edit Daily Quest' : 'Edit Main Quest');

                        if (type === 'daily' && !isSharedFromList) {
                            const goal = task.weeklyGoal || 0;
                            editWeeklyGoalSlider.value = goal;
                            editWeeklyGoalDisplay.textContent = goal > 0 ? `${goal}` : 'None';
                            editWeeklyGoalContainer.style.display = 'block';
                        } else {
                            editWeeklyGoalContainer.style.display = 'none';
                        }
                        openModal(editTaskModal);
                        focusOnDesktop(editTaskInput);
                    }
                } else {
                    // If the click was on the overlay but not on a specific button, close the actions.
                    hideActiveTaskActions();
                }
                return; // Click was inside the actions menu
            }

            // If we reach here, it was a click on the task body.
            if (type === 'daily' || type === 'shared') {
                if (isMyPartCompleted()) {
                    uncompleteDailyTask(id); // This function handles both daily and shared uncompletion
                } else {
                    completeTask(id); // This function handles both daily and shared completion
                }
            } else {
                completeTask(id); // Main quests are completed (and then deleted)
            }
        } 
    });

    addTaskForm.addEventListener('submit', (e) => { e.preventDefault(); const t = newTaskInput.value.trim(); if (t && currentListToAdd) { if (currentListToAdd.startsWith('shared-group-')) { const groupId = currentListToAdd.replace('shared-group-', ''); addTaskToSharedGroup(groupId, t); } else { const goal = (currentListToAdd === 'daily') ? parseInt(weeklyGoalSlider.value, 10) : 0; addTask(t, currentListToAdd, goal); } newTaskInput.value = ''; weeklyGoalSlider.value = 0; updateGoalDisplay(weeklyGoalSlider, weeklyGoalDisplay); closeModal(addTaskModal); } });
    editTaskForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const id = editTaskIdInput.value;
        const newText = editTaskInput.value.trim();
        if (!newText) return;

        try {
            const sharedId = JSON.parse(id);
            if (sharedId.groupId && sharedId.taskId) {
                editSharedTask(sharedId.groupId, sharedId.taskId, newText);
                closeModal(editTaskModal);
                return;
            }
        } catch (err) {
            // Not a shared task, proceed as normal
        }

        const newGoal = parseInt(editWeeklyGoalSlider.value, 10) || 0;
        if (id) {
            editTask(id, newText, newGoal);
            closeModal(editTaskModal);
        }
    });
    timerForm.addEventListener('submit', (e) => { e.preventDefault(); const v = parseInt(timerDurationSlider.value,10), u = timerUnitSelector.querySelector('.selected').dataset.unit; let m = 0; switch(u){ case 'seconds': m=v/60; break; case 'minutes': m=v; break; case 'hours': m=v*60; break; case 'days': m=v*1440; break; case 'weeks': m=v*10080; break; case 'months': m=v*43200; break; } if(m>0&&currentEditingTaskId){ if (currentEditingTaskId === 'batch_timer') { selectedQuestIds.forEach(id => { const { task } = findTaskAndContext(id); if (task && !task.isShared) { startTimer(id, m); } }); deactivateMultiSelectMode(); renderAllLists(); } else { startTimer(currentEditingTaskId,m); } closeModal(timerModal); currentEditingTaskId=null; } });
    timerMenuCancelBtn.addEventListener('click', () => { if (currentEditingTaskId) stopTimer(currentEditingTaskId); closeModal(timerMenuModal); });
    timerDurationSlider.addEventListener('input', () => timerDurationDisplay.textContent = timerDurationSlider.value);
    timerUnitSelector.addEventListener('click', (e) => { const t = e.target.closest('.timer-unit-btn'); if (t) { timerUnitSelector.querySelector('.selected').classList.remove('selected'); t.classList.add('selected'); audioManager.playSound('toggle'); } });
    addGroupForm.addEventListener('submit', (e) => { // eslint-disable-line
        e.preventDefault();
        const name = newGroupInput.value.trim();
        if (name) { if (currentEditingGroupId) { const isShared = sharedGroups.some(g => g.id === currentEditingGroupId); if (isShared) { editSharedGroupName(currentEditingGroupId, name); } else { editGroup(currentEditingGroupId, name); } } else { addGroup(name); } newGroupInput.value = ''; closeModal(addGroupModal); }
    });
    
    addTaskTriggerBtnDaily.addEventListener('click', () => { currentListToAdd = 'daily'; weeklyGoalContainer.style.display = 'block'; addTaskModalTitle.textContent = 'Add Daily Quest'; weeklyGoalSlider.value = 0; updateGoalDisplay(weeklyGoalSlider, weeklyGoalDisplay); openModal(addTaskModal); focusOnDesktop(newTaskInput); });
    addStandaloneTaskBtn.addEventListener('click', () => { currentListToAdd = 'standalone'; weeklyGoalContainer.style.display = 'none'; addTaskModalTitle.textContent = 'Add Main Quest'; openModal(addTaskModal); focusOnDesktop(newTaskInput); });
    addGroupBtn.addEventListener('click', () => {
        currentEditingGroupId = null;
        addGroupModal.querySelector('h2').textContent = 'Create New Group';
        addGroupModal.querySelector('.modal-submit-btn').textContent = 'Create';
        newGroupInput.value = '';
        openModal(addGroupModal);
        focusOnDesktop(newGroupInput);
    });
    settingsBtn.addEventListener('click', () => openModal(settingsModal));
    
    function handleFriendsModalClose() {
        mobileNav.querySelector('[data-section="friends"]').classList.remove('active');
        const lastSectionBtn = mobileNav.querySelector(`[data-section="${lastSection}"]`);
        if (lastSectionBtn) lastSectionBtn.classList.add('active');
        document.querySelectorAll('.task-group').forEach(group => {
            group.classList.toggle('mobile-visible', group.dataset.section === lastSection);
        });
    }

    document.querySelectorAll('[data-close-modal]').forEach(btn => btn.addEventListener('click', (e) => {
        const modalId = e.currentTarget.dataset.closeModal;
        const modal = document.getElementById(modalId);
        if (modal.getAttribute('data-persistent') !== 'true') {
            closeModal(modal);
            if (modalId === 'friends-modal') {
                handleFriendsModalClose();
            }
        }
    }));
    [addTaskModal, editTaskModal, addGroupModal, settingsModal, confirmModal, timerModal, accountModal, manageAccountModal, document.getElementById('username-modal'), document.getElementById('google-signin-loader-modal'), friendsModal, shareQuestModal, shareGroupModal, batchActionsModal].forEach(m => { 
        if (m) m.addEventListener('click', (e) => { 
            if (e.target === m && m.getAttribute('data-persistent') !== 'true') {
                closeModal(m); 
                if (m.id === 'friends-modal') {
                    handleFriendsModalClose();
                }
            }
        }); 
    });
    function showConfirm(title, text, cb) { confirmTitle.textContent = title; confirmText.textContent = text; confirmCallback = cb; openModal(confirmModal); }
    confirmActionBtn.addEventListener('click', () => { if (confirmCallback) confirmCallback(); closeModal(confirmModal); });
    confirmCancelBtn.addEventListener('click', () => closeModal(confirmModal));
    const applySettings = (e) => {
        // Determine target theme state
        const isCurrentlyDark = document.documentElement.classList.contains('dark-mode');
        const d = window.matchMedia('(prefers-color-scheme: dark)').matches;
        const willBeDark = settings.theme === 'dark' || (settings.theme === 'system' && d);

        // Apply non-theme settings immediately
        document.documentElement.style.setProperty('--accent', settings.accentColor);
        document.querySelectorAll('.color-swatch').forEach(s => s.classList.toggle('selected', s.dataset.color === settings.accentColor));
        if(typeof settings.volume === 'undefined') settings.volume = 0.3;
        volumeSlider.value = settings.volume;
        document.querySelectorAll('.theme-btn').forEach(b => b.classList.remove('selected'));
        const s = document.querySelector(`.theme-btn[data-theme="${settings.theme}"]`);
        if(s) s.classList.add('selected');

        // If theme isn't changing, we're done.
        if (isCurrentlyDark === willBeDark) return;

        // --- View Transition Logic ---
        if (!document.startViewTransition) {
            // Fallback for unsupported browsers
            document.documentElement.classList.toggle('dark-mode', willBeDark);
            return;
        }

        // Get click coordinates for the animation origin
        const x = e ? e.clientX : window.innerWidth / 2;
        const y = e ? e.clientY : window.innerHeight / 2;
        const endRadius = Math.hypot(
            Math.max(x, window.innerWidth - x),
            Math.max(y, window.innerHeight - y)
        );

        // Start the transition
        const transition = document.startViewTransition(() => {
            document.documentElement.classList.toggle('dark-mode', willBeDark);
        });

        // Animate the transition
        transition.ready.then(() => {
            document.documentElement.animate(
                { clipPath: [ `circle(0px at ${x}px ${y}px)`, `circle(${endRadius}px at ${x}px ${y}px)` ] },
                { duration: 500, easing: 'cubic-bezier(0.4, 0, 0.2, 1)', pseudoElement: '::view-transition-new(root)' }
            );
        });
    };
    themeOptionsButtons.addEventListener('click', (e) => { const t = e.target.closest('.theme-btn'); if (t) { settings.theme = t.dataset.theme; saveState(); applySettings(e); audioManager.playSound('toggle'); } });
    colorOptions.addEventListener('click', (e) => { if(e.target.classList.contains('color-swatch')) { settings.accentColor = e.target.dataset.color; saveState(); applySettings(); } });
    volumeSlider.addEventListener('input', () => { settings.volume = parseFloat(volumeSlider.value); saveState(); });
    volumeSlider.addEventListener('change', () => audioManager.playSound('toggle'));
    
    function updateGoalDisplay(slider, display) {
        const value = slider.value;
        if (value === '0') {
            display.textContent = 'None';
        } else {
            display.textContent = `${value} day${value > 1 ? 's' : ''}`;
        }
    }
    weeklyGoalSlider.addEventListener('input', () => updateGoalDisplay(weeklyGoalSlider, weeklyGoalDisplay));
    editWeeklyGoalSlider.addEventListener('input', () => updateGoalDisplay(editWeeklyGoalSlider, editWeeklyGoalDisplay));
    
    if (resetProgressBtn) resetProgressBtn.addEventListener('click', () => showConfirm('Reset all progress?', 'This cannot be undone.', () => { playerData = { level: 1, xp: 0 }; dailyTasks = []; standaloneMainQuests = []; generalTaskGroups = []; renderAllLists(); saveState(); audioManager.playSound('delete'); }));
    if (exportDataBtn) exportDataBtn.addEventListener('click', () => { const d = localStorage.getItem('anonymousUserData'); const b = new Blob([d || '{}'], {type: "application/json"}), a = document.createElement("a"); a.href = URL.createObjectURL(b); a.download = `procrasti-nope_guest_backup.json`; a.click(); });
    if (resetCloudDataBtn) {
        resetCloudDataBtn.addEventListener('click', () => {
            showConfirm('Reset all cloud data?', 'This will permanently erase all your quests and progress. This action cannot be undone.', () => {
                playerData = { level: 1, xp: 0 };
                dailyTasks = [];
                standaloneMainQuests = [];
                generalTaskGroups = [];
                renderAllLists();
                saveState(); // This will save the empty state to Firestore because `user` is not null
                audioManager.playSound('delete');
            });
        });
    }
    if (importDataBtn) importDataBtn.addEventListener('click', () => importFileInput.click());
    if (importFileInput) importFileInput.addEventListener('change', (e) => { const f = e.target.files[0]; if(!f) return; showConfirm("Import Guest Data?", "This will overwrite current guest data.", () => { const r = new FileReader(); r.onload = (e) => { localStorage.setItem('anonymousUserData', e.target.result); initialLoad(); }; r.readAsText(f); }); e.target.value = ''; });
    document.body.addEventListener('mouseover', e => { const t = e.target.closest('.btn, .color-swatch, .complete-btn, .main-title'); if (!t || (e.relatedTarget && t.contains(e.relatedTarget))) return; audioManager.playSound('hover'); });
    
    if (manageAccountBtn) {
        manageAccountBtn.addEventListener('click', () => {
            const reauthContainer = manageAccountModal.querySelector('#reauth-container');
            const manageFormsContainer = manageAccountModal.querySelector('#manage-forms-container');
            const isGoogleUser = currentUser && currentUser.providerData.some(p => p.providerId === 'google.com');

            manageAccountModal.querySelectorAll('.error-message, .success-message').forEach(el => el.textContent = '');
            manageAccountModal.querySelectorAll('form').forEach(form => form.reset());

            if (isGoogleUser) {
                reauthContainer.style.display = 'none';
                manageFormsContainer.style.display = 'block';
                manageAccountModal.querySelector('#update-email-form').style.display = 'none';
                manageAccountModal.querySelector('#update-password-form').style.display = 'none';
                manageAccountModal.querySelector('#update-username-form').style.display = 'block';
            } else {
                reauthContainer.style.display = 'block';
                manageFormsContainer.style.display = 'none';
            }
            openModal(manageAccountModal);
        });
    }

    const reauthForm = document.getElementById('reauth-form');
    if (reauthForm) {
        reauthForm.addEventListener('submit', async(e) => {
            e.preventDefault();
            const password = document.getElementById('reauth-password').value;
            const errorEl = document.getElementById('reauth-error');
            errorEl.textContent = '';

            if (!currentUser || !currentUser.email) {
                errorEl.textContent = 'No user is currently logged in.';
                return;
            }

            const credential = EmailAuthProvider.credential(currentUser.email, password);

            try {
                await reauthenticateWithCredential(currentUser, credential);
                document.getElementById('reauth-container').style.display = 'none';
                document.getElementById('manage-forms-container').style.display = 'block';
                // Explicitly show the forms for non-Google users after re-auth
                document.getElementById('update-username-form').style.display = 'block';
                document.getElementById('update-email-form').style.display = 'block';
                document.getElementById('update-password-form').style.display = 'block';
            } catch (error) {
                errorEl.textContent = getCoolErrorMessage(error);
            }
        });
    }

    const updateEmailForm = document.getElementById('update-email-form');
    if (updateEmailForm) {
        updateEmailForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const newEmail = document.getElementById('update-email-input').value;
            const password = document.getElementById('update-email-password').value;
            const errorEl = document.getElementById('update-email-error');
            const successEl = document.getElementById('update-email-success');
            errorEl.textContent = '';
            successEl.textContent = '';

            if (!password) {
                errorEl.textContent = 'Please enter your current password.';
                return;
            }

            try {
                const credential = EmailAuthProvider.credential(currentUser.email, password);
                await reauthenticateWithCredential(currentUser, credential);
                await updateEmail(currentUser, newEmail);
                
                const userDocRef = doc(db, "users", currentUser.uid);
                await updateDoc(userDocRef, { email: newEmail });

                successEl.textContent = 'Email updated successfully!';
                updateEmailForm.reset();

            } catch (error) {
                errorEl.textContent = getCoolErrorMessage(error);
            }
        });
    }

    const updatePasswordForm = document.getElementById('update-password-form');
    if (updatePasswordForm) {
        updatePasswordForm.addEventListener('submit', async(e) => {
            e.preventDefault();
            const newPassword = document.getElementById('update-password-input').value;
            const errorEl = document.getElementById('update-password-error');
            const successEl = document.getElementById('update-password-success');
            errorEl.textContent = '';
            successEl.textContent = '';

            try {
                await updatePassword(currentUser, newPassword);
                successEl.textContent = 'Password updated successfully!';
                updatePasswordForm.reset();
            } catch (error) {
                 errorEl.textContent = getCoolErrorMessage(error);
            }
        });
    }

    const updateUsernameForm = document.getElementById('update-username-form');
    if (updateUsernameForm) {
        updateUsernameForm.addEventListener('submit', async(e) => {
            e.preventDefault();
            const newUsername = document.getElementById('update-username-input').value.trim().toLowerCase();
            const errorEl = document.getElementById('update-username-error');
            const successEl = document.getElementById('update-username-success');
            errorEl.textContent = '';
            successEl.textContent = '';

            if (!currentUser) {
                errorEl.textContent = "You must be logged in.";
                return;
            }
            
            if (!newUsername || newUsername.length < 3) {
                errorEl.textContent = 'Username must be at least 3 characters.';
                return;
            }

            try {
                const userDocRef = doc(db, "users", currentUser.uid);
                const userDocSnap = await getDoc(userDocRef);
                const currentUsername = userDocSnap.exists() ? userDocSnap.data().username : null;

                if (newUsername === currentUsername) {
                    errorEl.textContent = "This is already your username.";
                    return;
                }
                
                const newUsernameRef = doc(db, "usernames", newUsername);
                const newUsernameSnap = await getDoc(newUsernameRef);
                
                if (newUsernameSnap.exists()) {
                    errorEl.textContent = "This username is already taken.";
                    return;
                }

                const oldUsernameRef = doc(db, "usernames", currentUsername);

                const batch = writeBatch(db);
                batch.delete(oldUsernameRef);
                batch.set(newUsernameRef, { userId: currentUser.uid });
                batch.update(userDocRef, { username: newUsername });
                await batch.commit();

                successEl.textContent = "Username updated successfully!";
                updateUserUI();
                updateUsernameForm.reset();
            } catch (error) {
                errorEl.textContent = getCoolErrorMessage(error);
            }
        });
    }
    
    function initSortable() {
        function onTaskDrop(evt) {
            // The onStart handler prevents shared tasks from being dragged.
            // By removing renderAllLists(), we get a smooth animation.
            // The data model is updated, and the DOM is handled by SortableJS.
            const taskId = evt.item.dataset.id;
            if (!taskId) return;

            const { task, list: sourceListArray } = findTaskAndContext(taskId);
            if (!task || !sourceListArray) return;

            const originalIndex = sourceListArray.findIndex(t => t.id === taskId);
            if (originalIndex > -1) {
                sourceListArray.splice(originalIndex, 1);
            } else {
                return; // Should not happen if findTaskAndContext is correct
            }

            const toListEl = evt.to;
            const toListId = toListEl.id;
            const toGroupId = toListEl.dataset.groupId;
            let destListArray;

            if (toListId === 'daily-task-list') {
                destListArray = dailyTasks;
            } else if (toListId === 'standalone-task-list') {
                destListArray = standaloneMainQuests;
            } else if (toGroupId) {
                const group = generalTaskGroups.find(g => g.id === toGroupId);
                if (group) {
                    if (!group.tasks) group.tasks = [];
                    destListArray = group.tasks;
                }
            }

            if (!destListArray) {
                // Dragged to an invalid location, put it back.
                sourceListArray.splice(originalIndex, 0, task);
                return;
            }
            
            destListArray.splice(evt.newIndex, 0, task);
            saveState();
            // NOTE: renderAllLists() was removed to allow for smooth animations.
            // This means a task's visual style won't update if dragged between
            // lists of different types until the next refresh. Since dragging is
            // restricted by group, this is a minor visual trade-off for smoothness.
        }

        const commonTaskOptions = {
            animation: 250, // Smoother animation
            delay: 150, // PERF: Reduced delay for a more responsive feel.
            delayOnTouchOnly: true, // PERF: Start drag immediately on desktop.
            onStart: (evt) => {
                const taskId = evt.item.dataset.id;
                const { task } = findTaskAndContext(taskId);
                if (task && task.isShared) {
                    evt.cancel = true;
                    return;
                }
                // FIX: Remove animation classes to prevent visual duplication from opacity conflicts.
                evt.item.classList.remove('adding', 'removing');
                // PERF: Removed adding a class to body to prevent expensive global animations.
            },
            onEnd: onTaskDrop 
        };

        new Sortable(dailyTaskListContainer, { ...commonTaskOptions, group: 'dailyQuests' });
        new Sortable(standaloneTaskListContainer, { ...commonTaskOptions, group: 'mainQuests' });
        document.querySelectorAll('.task-list-group').forEach(listEl => {
            new Sortable(listEl, { ...commonTaskOptions, group: 'mainQuests' });
        });
        new Sortable(generalTaskListContainer, {
            animation: 250, // Smoother animation
            handle: '.main-quest-group-header',
            delay: 150, // PERF: Reduced delay.
            delayOnTouchOnly: true,
            onStart: (evt) => {
                // FIX: Remove animation classes to prevent visual duplication from opacity conflicts.
                evt.item.classList.remove('adding', 'removing');
            },
            onEnd: (e) => {
                const [item] = generalTaskGroups.splice(e.oldIndex, 1);
                generalTaskGroups.splice(e.newIndex, 0, item);
                saveState();
            }
        });
    }

    function createConfetti(el) { if(!el) return; const r = el.getBoundingClientRect(); createFullScreenConfetti(false, { x: r.left + r.width / 2, y: r.top + r.height / 2 }); }
    function createFullScreenConfetti(party, o = null) {
        for (let i = 0; i < (party ? 150 : 50); i++) { // PERF: Reduced confetti particle count
            const c = document.createElement('div'); c.className = 'confetti';
            const sx = o ? o.x : Math.random()*window.innerWidth, sy = o ? o.y : -20;
            c.style.left=`${sx}px`; c.style.top=`${sy}px`; c.style.backgroundColor = ['var(--accent-pink)','var(--accent-blue)','var(--accent-green)','var(--accent-orange)','var(--accent-purple)'][Math.floor(Math.random()*5)];
            document.body.appendChild(c);
            const a = Math.random()*Math.PI*2, v=50+Math.random()*100, ex=Math.cos(a)*v*(Math.random()*5), ey=(Math.sin(a)*v)+(window.innerHeight-sy);
            c.animate([{transform:'translate(0,0) rotate(0deg)',opacity:1},{transform:`translate(${ex}px, ${ey}px) rotate(${Math.random()*720}deg)`,opacity:0}],{duration:3000+Math.random()*2000,easing:'cubic-bezier(0.1,0.5,0.5,1)'}).onfinish=()=>c.remove();
        }
        if(party){const p=document.createElement('div');p.className='party-time-overlay';document.body.appendChild(p);setTimeout(()=>p.remove(),5000);}
    }
    const renderAllLists = () => { renderSharedItems(); renderDailyTasks(); renderStandaloneTasks(); renderGeneralTasks(); renderIncomingItems(); initSortable(); resumeTimers(); };
    
    function setInitialActiveTab() {
        // The animation for the nav buttons is the longest, finishing around 0.4s (delay) + 0.4s (duration) = 0.8s.
        // We wait until after this animation to add the 'active' class. This prevents the class's styles
        // from interfering with the entrance animation and provides a nice "settling" effect.
        setTimeout(() => {
            const dailyBtn = mobileNav.querySelector('[data-section="daily"]');
            // By default, the 'daily' tab is active. Ensure no other buttons are active
            // and apply the class to the correct button.
            if (dailyBtn && !dailyBtn.classList.contains('active')) {
                mobileNav.querySelectorAll('.mobile-nav-btn.active').forEach(btn => btn.classList.remove('active'));
                dailyBtn.classList.add('active');
            }
        }, 850); // A safe delay after the last animation finishes.
    }

    settingsLoginBtn.addEventListener('click', () => {
        const accountModalContent = accountModal.querySelector('.modal-content');
        setupAuthForms(accountModalContent, () => {
            closeModal(accountModal);
            closeModal(settingsModal);
        });
        openModal(accountModal);
    });

    logoutBtn.addEventListener('click', () => {
        showConfirm("Logout?", "You will be returned to the landing page.", () => {
            closeModal(settingsModal);
            sessionStorage.removeItem('isGuest'); 
            localStorage.removeItem('userTheme');
            signOut(auth).catch(error => console.error("Logout Error:", getCoolErrorMessage(error)));
        });
    });
    
    // NEW: Combined listener for friend requests and incoming shared quests
    function listenForFriendsAndShares() {
        if (!user) return;
        if (unsubscribeFromFriendsAndShares) {
            unsubscribeFromFriendsAndShares();
            unsubscribeFromFriendsAndShares = null;
        }

        const listeners = [];

        // Listener for the user's own document to get their friends list
        const userDocRef = doc(db, "users", user.uid);
        listeners.push(onSnapshot(userDocRef, (docSnap) => { // This listener handles confirmed friends
            if (docSnap.exists()) {
                const userData = docSnap.data();
                confirmedFriendUIDs = userData.friends || [];
                debouncedRenderFriends();
            }
        }));

        // Listener for ALL friend requests (incoming and outgoing)
        const allRequestsQuery = query(collection(db, "friendRequests"), where("participants", "array-contains", user.uid));
        listeners.push(onSnapshot(allRequestsQuery, async (snapshot) => {
            // REFACTOR: This logic is now split. The recipient initiates 'accept' or 'decline' by updating
            // the request status. The sender's client finalizes the action upon seeing the status change.
            for (const change of snapshot.docChanges()) {
                if (change.type === 'modified') {
                    const requestData = change.doc.data();
                    const requestRef = doc(db, "friendRequests", change.doc.id);

                    // I am the SENDER, and the recipient just ACCEPTED.
                    if (user && requestData.senderUid === user.uid && requestData.status === 'accepted') {
                        // The recipient has already added us. Now we add them and delete the request.
                        const batch = writeBatch(db);
                        const currentUserRef = doc(db, "users", user.uid);
                        
                        batch.update(currentUserRef, { friends: arrayUnion(requestData.recipientUid) });
                        batch.delete(requestRef);
                        
                        await batch.commit();
                    } 
                    // I am the SENDER, and the recipient just DECLINED.
                    else if (user && requestData.senderUid === user.uid && requestData.status === 'declined') {
                        // The recipient has indicated they don't want to be friends. We just clean up the request.
                        await deleteDoc(requestRef);
                    }
                }
            }

            // Repopulate local lists from the full snapshot
            const allRequests = snapshot.docs.map(d => ({ ...d.data(), id: d.id }));

            incomingFriendRequests = allRequests.filter(req => req.recipientUid === user.uid && req.status === 'pending');
            outgoingFriendRequests = allRequests.filter(req => req.senderUid === user.uid && req.status === 'pending');

            // Render the UI for incoming requests
            renderIncomingRequests(incomingFriendRequests);

            // Update notification badges for incoming requests
            const requestCount = incomingFriendRequests.length;
            const modalBadge = friendRequestCountBadgeModal;
            if (modalBadge) {
                if (requestCount > 0) {
                    modalBadge.textContent = requestCount;
                    modalBadge.style.display = 'flex';
                } else {
                    modalBadge.style.display = 'none';
                }
            }
            updateMainNotificationBadges();

            // Trigger a debounced render to update the list with new pending requests
            debouncedRenderFriends();
        }));

        // NEW: Listener for friend removals initiated by other users.
        const removalsQuery = query(collection(db, "friendRemovals"), where("removeeUid", "==", user.uid));
        listeners.push(onSnapshot(removalsQuery, async (snapshot) => {
            if (snapshot.empty) return;

            // Use a write batch to handle all Firestore changes atomically for this snapshot.
            const batch = writeBatch(db);
            let localStateChanged = false;

            for (const change of snapshot.docChanges()) {
                if (change.type !== 'added') continue;

                const removalDoc = change.doc;
                const removalData = removalDoc.data();
                const removerUid = removalData.removerUid;
                // Use the new sharedQuestsData package instead of just IDs
                const sharedQuestsData = removalData.sharedQuestsData || [];
                const sharedGroupsData = removalData.sharedGroupsData || [];

                // Revert any quests owned by the current user (the one being removed)
                if (sharedQuestsData.length > 0) {
                    for (const questData of sharedQuestsData) {
                        if (questData.ownerUid === user.uid) {
                            const { task } = findTaskAndContext(questData.originalTaskId);
                            if (task) {
                                task.isShared = false;
                                delete task.sharedQuestId;
                                localStateChanged = true;
                            }
                        }
                    }
                }

                // NEW: Revert any groups owned by the current user (the one being removed)
                if (sharedGroupsData.length > 0) {
                    for (const groupData of sharedGroupsData) {
                        if (groupData.ownerUid === user.uid) {
                            const originalGroup = generalTaskGroups.find(g => g.id === groupData.originalGroupId);
                            if (originalGroup) {
                                delete originalGroup.isShared;
                                delete originalGroup.sharedGroupId;
                                localStateChanged = true;
                            }
                        }
                    }
                }

                // Add operations to the main batch
                const currentUserRef = doc(db, "users", user.uid);
                batch.update(currentUserRef, { friends: arrayRemove(removerUid) });

                // Delete the shared quests using the IDs from the data package
                const sharedQuestIdsToDelete = sharedQuestsData.map(q => q.id);
                sharedQuestIdsToDelete.forEach(id => batch.delete(doc(db, "sharedQuests", id)));

                // NEW: Delete the shared groups
                const sharedGroupIdsToDelete = sharedGroupsData.map(g => g.id);
                sharedGroupIdsToDelete.forEach(id => batch.delete(doc(db, "sharedGroups", id)));

                // Delete the removal trigger document itself
                batch.delete(removalDoc.ref);
            }

            // Commit all batched writes for this snapshot at once.
            await batch.commit().catch(error => {
                console.error("Error processing friend removal batch:", getCoolErrorMessage(error));
            });

            // If local state was changed, save and re-render after the batch commit.
            if (localStateChanged) {
                saveState();
                renderAllLists();
            }
        }));

        unsubscribeFromFriendsAndShares = () => listeners.forEach(unsub => unsub());
    }

    async function renderFriendsList() {
        if (!user) return;

        friendsListContainer.innerHTML = ''; // Start fresh

        // Defensively ensure friendUIDs are unique to prevent issues from upstream.
        const uniqueFriendUIDs = confirmedFriendUIDs ? [...new Set(confirmedFriendUIDs)] : [];

        // 1. Fetch confirmed friends data
        let confirmedFriends = [];
        if (uniqueFriendUIDs.length > 0) {
            const friendsQuery = query(collection(db, "users"), where(documentId(), 'in', uniqueFriendUIDs));
            const friendDocs = await getDocs(friendsQuery);
            confirmedFriends = friendDocs.docs.map(doc => {
                const friend = doc.data();
                const level = friend.appData?.playerData?.level || 1;
                return { type: 'friend', uid: doc.id, username: friend.username, level: level };
            });
        }

        // 2. Prepare outgoing requests data (from global state)
        const pendingFriends = outgoingFriendRequests.map(req => ({
            type: 'pending',
            requestId: req.id,
            uid: req.recipientUid,
            username: req.recipientUsername
        }));

        // 3. Combine and de-duplicate the lists.
        // This ensures that if a user is both a confirmed friend and has a pending request
        // (due to a race condition), they only appear once as a confirmed friend.
        const allItemsCombined = [...confirmedFriends, ...pendingFriends];
        const seenUids = new Set();
        const allItems = allItemsCombined.filter(item => {
            if (seenUids.has(item.uid)) return false; // Already seen, so it's a duplicate pending request.
            seenUids.add(item.uid);
            return true;
        });

        if (allItems.length === 0) {
            friendsListContainer.innerHTML = `<p style="text-align: center; padding: 1rem;">Go add some friends!</p>`;
            return;
        }

        allItems.forEach(item => {
            const itemEl = document.createElement('div');
            itemEl.className = 'friend-item';

            if (item.type === 'friend') {
                itemEl.innerHTML = `
                    <div class="friend-level-display">LVL ${item.level}</div>
                    <span class="friend-name">${item.username}</span>
                    <div class="friend-item-actions">
                        <button class="btn icon-btn remove-friend-btn" data-uid="${item.uid}" aria-label="Remove friend">&times;</button>
                    </div>`;
            } else { // type === 'pending'
                itemEl.classList.add('pending');
                itemEl.innerHTML = `
                    <div class="pending-tag">Pending</div>
                    <span class="friend-name">${item.username}</span>
                    <div class="friend-item-actions">
                        <button class="btn icon-btn cancel-request-btn" data-id="${item.requestId}" aria-label="Cancel request">&times;</button>
                    </div>`;
            }
            friendsListContainer.appendChild(itemEl);
        });
    }

    function renderIncomingRequests(requestObjects) {
        if (requestObjects.length === 0) {
            friendRequestsContainer.innerHTML = `<p style="text-align: center; padding: 1rem;">No new requests.</p>`;
        } else {
            friendRequestsContainer.innerHTML = '';
            requestObjects.forEach(req => {
                const requestEl = document.createElement('div');
                requestEl.className = 'friend-request-item';
                requestEl.innerHTML = `<span>${req.senderUsername}</span><div class="friend-request-actions"><button class="btn icon-btn accept-request-btn" data-id="${req.id}" data-uid="${req.senderUid}" aria-label="Accept request">&#10003;</button><button class="btn icon-btn decline-request-btn" data-id="${req.id}" data-uid="${req.senderUid}" aria-label="Decline request">&times;</button></div>`;
                friendRequestsContainer.appendChild(requestEl);
            });
        }
    }

    async function handleAddFriend(e) {
        e.preventDefault();
        const usernameToFind = searchUsernameInput.value.trim().toLowerCase();
        friendStatusMessage.textContent = '';
        
        if (!user || !usernameToFind) return;

        const currentUserDoc = await getDoc(doc(db, "users", user.uid));
        const currentUserData = currentUserDoc.exists() ? currentUserDoc.data() : {};
        const currentUsername = currentUserData.username || null;
        const currentFriends = currentUserData.friends || [];
        
        if (!currentUsername) {
            friendStatusMessage.textContent = "Error: Your username is not set. Cannot send request.";
            friendStatusMessage.style.color = 'var(--accent-red-light)';
            // The app should have prompted for a username on login, but as a fallback:
            await promptForUsernameIfNeeded();
            return;
        }

        if (currentUsername && usernameToFind === currentUsername.toLowerCase()) {
            friendStatusMessage.textContent = "You can't send a friend request to yourself!";
            friendStatusMessage.style.color = 'var(--accent-red-light)';
            return;
        }
        
        const usernamesRef = doc(db, "usernames", usernameToFind);
        const usernameSnap = await getDoc(usernamesRef);

        if (!usernameSnap.exists()) {
            friendStatusMessage.textContent = "User not found.";
            friendStatusMessage.style.color = 'var(--accent-red-light)';
            return;
        }
        
        const targetUserId = usernameSnap.data().userId;

        // Check if the target user is already a friend.
        if (currentFriends.includes(targetUserId)) {
            friendStatusMessage.textContent = `You are already friends with ${usernameToFind}.`;
            friendStatusMessage.style.color = 'var(--accent-red-light)';
            return;
        }

        // REFACTOR: Use a canonical ID for the friend request to prevent duplicates.
        const canonicalRequestId = [user.uid, targetUserId].sort().join('_');
        const requestDocRef = doc(db, "friendRequests", canonicalRequestId);

        try {
            // REFACTOR: The security rules prevent checking if a document exists before writing it.
            // Instead, we attempt to create it. If it fails with "permission-denied", it's because
            // the document already exists (making it an update, which is denied for the sender),
            // so we can infer that a request is already pending.
            await setDoc(requestDocRef, {
                senderUid: user.uid,
                senderUsername: currentUsername,
                recipientUid: targetUserId,
                recipientUsername: usernameToFind,
                participants: [user.uid, targetUserId],
                status: 'pending',
                createdAt: Date.now()
            });
            friendStatusMessage.textContent = `Friend request sent to ${usernameToFind}!`;
            friendStatusMessage.style.color = 'var(--accent-green-light)';
            searchUsernameInput.value = '';
        } catch (error) {
            if (error.code === 'permission-denied') {
                friendStatusMessage.textContent = "A friend request is already pending with this user.";
            } else {
                friendStatusMessage.textContent = "Could not send request.";
                console.error("Error sending friend request:", getCoolErrorMessage(error));
            }
            friendStatusMessage.style.color = 'var(--accent-red-light)';
        }
    }
    
    async function cancelSentRequest(requestId) {
        if (!requestId) return;
        try {
            await deleteDoc(doc(db, "friendRequests", requestId));
            audioManager.playSound('delete');
            // The onSnapshot listener will automatically update the UI.
        } catch (error) {
            console.error("Error cancelling friend request:", getCoolErrorMessage(error));
            friendStatusMessage.textContent = "Could not cancel request.";
            friendStatusMessage.style.color = 'var(--accent-red-light)';
        }
    }

    async function handleRequestAction(e, action) {
        const button = e.target.closest('button');
        if (!button) return;

        const requestId = button.dataset.id;
        const senderUid = button.dataset.uid;
        const recipientUid = user.uid; // The current user is the recipient
        const requestDocRef = doc(db, "friendRequests", requestId);

        if (action === 'accept') {
            // REFACTOR: Use a two-step process compliant with security rules.
            // 1. The recipient (current user) adds the sender to their friends list.
            // 2. The recipient updates the request status to 'accepted'.
            // 3. The sender's client will be listening for this status change to complete the process.
            const batch = writeBatch(db);
            const recipientUserRef = doc(db, "users", recipientUid);

            batch.update(recipientUserRef, { friends: arrayUnion(senderUid) });
            batch.update(requestDocRef, { status: 'accepted' });

            await batch.commit();
            // The onSnapshot listeners on both clients will see the changes and update UIs.
        } else { // decline
            // REFACTOR: The recipient cannot delete the request directly due to security rules.
            // They update the status, and the sender's client will delete it.
            await updateDoc(requestDocRef, { status: 'declined' });
        }
    }
    
    async function removeFriend(e) {
        const button = e.target.closest('button');
        if (!button) return;
        const friendUidToRemove = button.dataset.uid;

        showConfirm("Remove Friend?", "Shared quests and groups will be removed for both of you. Are you sure?", async () => {
            // This function is now optimistic. It updates the UI instantly and then syncs with the server.
            const currentUserRef = doc(db, "users", user.uid);

            // 1. Find all shared items between the two users.
            const q1 = query(collection(db, "sharedQuests"), where("participants", "==", [user.uid, friendUidToRemove]));
            const q2 = query(collection(db, "sharedQuests"), where("participants", "==", [friendUidToRemove, user.uid]));
            const qg1 = query(collection(db, "sharedGroups"), where("participants", "==", [user.uid, friendUidToRemove]));
            const qg2 = query(collection(db, "sharedGroups"), where("participants", "==", [friendUidToRemove, user.uid]));
            
            let snap1, snap2, snapg1, snapg2;
            try {
                [snap1, snap2, snapg1, snapg2] = await Promise.all([
                    getDocs(q1), getDocs(q2), getDocs(qg1), getDocs(qg2)
                ]);
            } catch (error) {
                console.error("Error fetching shared items for removal:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not fetch shared items to remove. Please check your connection and try again.", () => {});
                return; // Stop if fetching failed
            }

            const sharedQuestDocs = [...snap1.docs, ...snap2.docs];
            const sharedGroupDocs = [...snapg1.docs, ...snapg2.docs];

            // 2. Separate items by ownership to respect security rules.
            const questsIOwn = [];
            const questsTheyOwnData = [];
            sharedQuestDocs.forEach(d => {
                const data = d.data();
                if (data.ownerUid === user.uid) {
                    questsIOwn.push({ doc: d, data: data });
                } else {
                    questsTheyOwnData.push({ id: d.id, ownerUid: data.ownerUid, originalTaskId: data.originalTaskId });
                }
            });

            const groupsIOwn = [];
            const groupsTheyOwnData = [];
            sharedGroupDocs.forEach(d => {
                const data = d.data();
                if (data.ownerUid === user.uid) {
                    groupsIOwn.push({ doc: d, data: data });
                } else {
                    groupsTheyOwnData.push({ id: d.id, ownerUid: data.ownerUid, originalGroupId: data.originalGroupId });
                }
            });

            // 3. Revert local state for items I own.
            let localStateChanged = false;
            questsIOwn.forEach(item => {
                const { task } = findTaskAndContext(item.data.originalTaskId);
                if (task) {
                    task.isShared = false;
                    delete task.sharedQuestId;
                    localStateChanged = true;
                }
            });
            groupsIOwn.forEach(item => {
                const originalGroup = generalTaskGroups.find(g => g.id === item.data.originalGroupId);
                if (originalGroup) {
                    delete originalGroup.isShared;
                    delete originalGroup.sharedGroupId;
                    localStateChanged = true;
                }
            });

            // --- OPTIMISTIC UI UPDATE ---
            // Apply local changes and re-render the UI immediately.
            if (localStateChanged) {
                saveState();
                renderAllLists();
            }

            // 4. Create a batch to update Firestore.
            const batch = writeBatch(db);

            // Clean up any pending friend requests between the users.
            // This ensures a clean state and handles edge cases where a request might be stale.
            const canonicalRequestId = [user.uid, friendUidToRemove].sort().join('_');
            const requestDocRef = doc(db, "friendRequests", canonicalRequestId);
            batch.delete(requestDocRef);

            // 4a. Delete items I own directly.
            questsIOwn.forEach(item => batch.delete(item.doc.ref));
            groupsIOwn.forEach(item => batch.delete(item.doc.ref));

            // 4b. Update my own friends list.
            batch.update(currentUserRef, { friends: arrayRemove(friendUidToRemove) });

            // 4c. Create the removal trigger document for items THEY own.
            const removalId = [user.uid, friendUidToRemove].sort().join('_');
            const removalRef = doc(db, "friendRemovals", removalId);
            batch.set(removalRef, {
                removerUid: user.uid,
                removeeUid: friendUidToRemove,
                sharedQuestsData: questsTheyOwnData, // Only items they own
                sharedGroupsData: groupsTheyOwnData, // Only items they own
                createdAt: Date.now()
            });

            // 5. Commit the changes to the server.
            try {
                await batch.commit();
                // On success, the UI is already correct. The snapshot listeners will just confirm the state.
            } catch (error) {
                // On failure, the UI is now out of sync. We must inform the user and reload to get the correct state.
                console.error("Failed to remove friend and shares:", getCoolErrorMessage(error));
                showConfirm("Update Failed", "Could not remove friend from the server. Reloading to sync your data.", () => {
                    window.location.reload();
                });
            }
        });
    }

    const unshareSharedGroup = async (groupId) => {
        const group = sharedGroups.find(g => g.id === groupId);
        if (!group) return;

        if (user.uid !== group.ownerUid) {
            showConfirm("Cannot Unshare", "Only the owner can unshare a group.", () => {});
            return;
        }

        showConfirm("Unshare Group?", "This will convert it back to a normal group for you and remove it for your friend. Are you sure?", async () => {
            try {
                // Re-create the group in the owner's local list.
                const newLocalGroup = { id: group.originalGroupId, name: group.name, tasks: group.tasks.map(st => ({ id: st.id, text: st.text, createdAt: Date.now(), isShared: false })), isExpanded: false };
                generalTaskGroups.push(newLocalGroup);
                saveState();

                await deleteDoc(doc(db, "sharedGroups", groupId));
                audioManager.playSound('delete');
                renderAllLists();
            } catch (error) {
                console.error("Error unsharing group:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not unshare the group.", () => {});
            }
        });
    };

    const abandonSharedGroup = async (groupId) => {
        const group = sharedGroups.find(g => g.id === groupId);
        if (!group || (user && group.ownerUid === user.uid)) return;

        showConfirm("Abandon Group?", "This will remove the group from your list and the owner will be notified. Are you sure?", async () => {
            try {
                const sharedGroupRef = doc(db, "sharedGroups", groupId);
                await updateDoc(sharedGroupRef, { status: 'abandoned' });
                audioManager.playSound('delete');
            } catch (error) {
                console.error("Error abandoning group:", getCoolErrorMessage(error));
                showConfirm("Error", "Could not abandon the group.", () => {});
            }
        });
    };

    friendsBtnDesktop.addEventListener('click', () => {
        openModal(friendsModal);
        // The listener will trigger renderFriendsAndRequests and renderIncomingShares
    });
    
    addFriendForm.addEventListener('submit', handleAddFriend);
    
    friendRequestsContainer.addEventListener('click', e => {
         if (e.target.closest('.accept-request-btn')) handleRequestAction(e, 'accept');
         if (e.target.closest('.decline-request-btn')) handleRequestAction(e, 'decline');
    });
    
    friendsListContainer.addEventListener('click', e => {
        if (e.target.closest('.remove-friend-btn')) removeFriend(e);
        const cancelButton = e.target.closest('.cancel-request-btn');
        if (cancelButton) {
            const requestId = cancelButton.dataset.id;
            cancelSentRequest(requestId);
        }
    });

    friendsModalToggle.addEventListener('click', (e) => {
        if (e.target.matches('.toggle-btn')) {
            const tab = e.target.dataset.tab;
            friendsModalToggle.querySelectorAll('.toggle-btn').forEach(btn => btn.classList.remove('active'));
            e.target.classList.add('active');
            friendsModal.querySelectorAll('.tab-content').forEach(form => form.classList.toggle('active', form.id === `${tab}-tab`));
            audioManager.playSound('open');
        }
    });

    mobileNav.addEventListener('click', (e) => {
        const button = e.target.closest('.mobile-nav-btn');
        if (!button) return;

        const section = button.dataset.section;
        
        if (section !== 'friends') {
            lastSection = section;
        }

        mobileNav.querySelectorAll('.mobile-nav-btn').forEach(btn => btn.classList.remove('active'));
        button.classList.add('active');

        if (section === 'friends') {
            document.querySelectorAll('.task-group').forEach(group => {
                group.classList.remove('mobile-visible');
            });
            openModal(friendsModal);
            // The listener will trigger renderFriendsAndRequests and renderIncomingShares
        } else {
            document.querySelectorAll('.task-group').forEach(group => {
                group.classList.toggle('mobile-visible', group.dataset.section === section);
            });
        }
        audioManager.playSound('toggle');
    });

    deleteAccountBtn.addEventListener('click', () => {
        // Get the new error element and clear it before showing the confirmation.
        const deleteAccountErrorEl = document.getElementById('delete-account-error');
        if (deleteAccountErrorEl) deleteAccountErrorEl.textContent = '';

        showConfirm('Delete Account?', 'This action is irreversible and will permanently delete your account and all associated data.', async () => {
            try {
                // The user has already re-authenticated to get to this screen.
                // The deleteUser call below will fail with 'auth/requires-recent-login'
                // if the token has expired, which is the correct security behavior.

                const userDocRef = doc(db, "users", currentUser.uid);
                const userDocSnap = await getDoc(userDocRef);
                const username = userDocSnap.exists() ? userDocSnap.data().username : null;

                const batch = writeBatch(db);
                batch.delete(userDocRef);
                if (username) {
                    const usernameDocRef = doc(db, "usernames", username);
                    batch.delete(usernameDocRef);
                }

                // This is the sensitive operation that requires recent login.
                await deleteUser(currentUser);
                
                // This commit will only run if deleteUser was successful.
                await batch.commit();

                closeModal(manageAccountModal);
                signOut(auth);
                window.location.reload(); 
            } catch (error) {
                console.error("Error deleting account:", error);
                closeModal(confirmModal); // Close the 'are you sure' modal first.

                if (error.code === 'auth/requires-recent-login') {
                    // If re-auth is needed, send the user back to the password confirmation screen.
                    document.getElementById('reauth-container').style.display = 'block';
                    document.getElementById('manage-forms-container').style.display = 'none';
                    const reauthErrorEl = document.getElementById('reauth-error');
                    if (reauthErrorEl) reauthErrorEl.textContent = "For security, please confirm your password again to delete your account.";
                } else {
                    // For any other error, display it in the dedicated error field.
                    const deleteErrorEl = document.getElementById('delete-account-error');
                    if (deleteErrorEl) deleteErrorEl.textContent = getCoolErrorMessage(error);
                }
            }
        });
    });

    async function loadUserSession() {
        await initialLoad();
        await updateUserUI();
        await promptForUsernameIfNeeded();
        await updateUserUI();
        checkDailyReset();
        resumeTimers();
        setInitialActiveTab(); // Set the initial active tab after everything is loaded.
    }
    
    // --- SHARED QUESTS LOGIC ---
    
    async function populateFriendListForSharing(listElement) {
        if (!user) {
            listElement.innerHTML = '<p style="text-align: center; padding: 1rem;">You must be logged in to share.</p>';
            return;
        }
        
        listElement.innerHTML = '<div class="loader-box" style="margin: 2rem auto;"></div>';

        const userDoc = await getDoc(doc(db, "users", user.uid));
        const friendUIDs = userDoc.data()?.friends || [];
        
        if (friendUIDs.length === 0) {
            listElement.innerHTML = '<p style="text-align: center; padding: 1rem;">You need friends to share with!</p>';
            return;
        }

        listElement.innerHTML = '';
        const friendsQuery = query(collection(db, "users"), where(documentId(), 'in', friendUIDs));
        const friendDocs = await getDocs(friendsQuery);

        friendDocs.forEach(friendDoc => {
            const friendData = friendDoc.data();
            const friendEl = document.createElement('div');
            friendEl.className = 'share-friend-item';
            friendEl.innerHTML = `
                <span class="friend-name">${friendData.username}</span>
                <button class="btn share-btn-action" data-uid="${friendDoc.id}" data-username="${friendData.username}">Share</button>
                <div class="friend-level-display">LVL ${friendData.appData?.playerData?.level || 1}</div>
            `;
            listElement.appendChild(friendEl);
        });
    }
    function listenForSharedQuests() {
        if (!user) return;
        if (unsubscribeFromSharedQuests) unsubscribeFromSharedQuests();

        // Use the map from the parent scope to make it accessible elsewhere (e.g., for orphan checks)
        questsMap.clear(); // Clear it on re-listen
        let unsubscribers = [];

        const handleSnapshot = (querySnapshot) => { // PERF: Combined multiple listeners into one.
            // Process changes to update the map
            querySnapshot.docChanges().forEach((change) => {
                if (change.type === 'removed') {
                    questsMap.delete(change.doc.id);
                } else { // 'added' or 'modified'
                    const newQuest = { ...change.doc.data(), id: change.doc.id, questId: change.doc.id }; // questId is redundant but harmless

                    // Handle termination events (rejection/abandonment by friend, or unshare by owner)
                    if ((newQuest.status === 'rejected' || newQuest.status === 'abandoned' || newQuest.status === 'unshared') && newQuest.ownerUid === user.uid) {
                        revertSharedQuest(newQuest.originalTaskId);
                        deleteDoc(doc(db, "sharedQuests", newQuest.id));
                        questsMap.delete(change.doc.id); // Ensure it's removed from the local map
                        return; // Stop processing this change
                    }

                    // NEW: If a quest is newly marked as 'completed', trigger the finish animation for both users.
                    if (newQuest.status === 'completed') {
                        const oldQuest = questsMap.get(change.doc.id);
                        if (!oldQuest || oldQuest.status !== 'completed') {
                            finishSharedQuestAnimation(newQuest);
                        }
                    }

                    const oldQuest = questsMap.get(change.doc.id);
                    if (oldQuest && change.type === 'modified') {
                         const isOwner = newQuest.ownerUid === user.uid;
                         const friendJustCompleted = (isOwner && !oldQuest.friendCompleted && newQuest.friendCompleted) ||
                                                     (!isOwner && !oldQuest.ownerCompleted && newQuest.ownerCompleted);
                         if (friendJustCompleted) {
                             const taskEl = document.querySelector(`.task-item[data-id="${newQuest.id}"]`);
                             if(taskEl) {
                                taskEl.classList.add('friend-completed-pulse');
                                taskEl.addEventListener('animationend', () => taskEl.classList.remove('friend-completed-pulse'), {once: true});
                                audioManager.playSound('friendComplete');
                             }
                        }
                    }
                    questsMap.set(change.doc.id, newQuest);
                }
            });
            
            const allQuestsFromListener = Array.from(questsMap.values());
        
            // Filter for active/completed quests for the main shared list
            sharedQuests = allQuestsFromListener.filter(q => q.status === 'active' || q.status === 'completed');
    
            // Filter for pending quests for the incoming shares tab
            const incomingQuests = allQuestsFromListener.filter(q => q.status === 'pending' && q.friendUid === user.uid);
            const incomingGroups = allSharedGroupsFromListener.filter(g => g.status === 'pending' && g.friendUid === user.uid);
            incomingSharedItems = [...incomingQuests, ...incomingGroups];

            updateMainNotificationBadges();
            renderAllLists();
        };

        // A single listener for all relevant statuses
        const allSharedQuestsQuery = query(
            collection(db, "sharedQuests"), 
            where("participants", "array-contains", user.uid),
            where("status", "in", ["pending", "active", "completed", "rejected", "abandoned", "unshared"])
        );
        unsubscribers.push(onSnapshot(allSharedQuestsQuery, handleSnapshot, (error) => {
            console.error("Error listening for shared quests:", getCoolErrorMessage(error));
        }));

        unsubscribeFromSharedQuests = () => unsubscribers.forEach(unsub => unsub());
    }

    function listenForSharedGroups() {
        if (!user) return;
        if (unsubscribeFromSharedGroups) unsubscribeFromSharedGroups();

        let groupsMap = new Map();
        const q = query(
            collection(db, "sharedGroups"),
            where("participants", "array-contains", user.uid),
            where("status", "in", ["pending", "active", "completed", "rejected", "abandoned"])
        );

        unsubscribeFromSharedGroups = onSnapshot(q, (snapshot) => {
            snapshot.docChanges().forEach((change) => {
                if (change.type === 'removed') {
                    groupsMap.delete(change.doc.id);
                } else {
                    const newGroup = { ...change.doc.data(), id: change.doc.id };

                    // Preserve expanded state across re-renders from Firestore
                    const oldGroup = groupsMap.get(change.doc.id);
                    if (oldGroup && oldGroup.isExpanded) {
                        newGroup.isExpanded = true;
                    }

                    // Handle when a friend rejects or abandons a group share
                    if ((newGroup.status === 'rejected' || newGroup.status === 'abandoned') && newGroup.ownerUid === user.uid) {
                        // Re-create the group locally for the owner.
                        const newLocalGroup = { id: newGroup.originalGroupId, name: newGroup.name, tasks: newGroup.tasks.map(st => ({ id: st.id, text: st.text, createdAt: Date.now(), isShared: false })), isExpanded: false };
                        generalTaskGroups.push(newLocalGroup);
                        saveState();

                        deleteDoc(doc(db, "sharedGroups", newGroup.id));
                        groupsMap.delete(change.doc.id);
                        return;
                    }

                    if (newGroup.status === 'completed') {
                        const oldGroup = groupsMap.get(change.doc.id);
                        if (!oldGroup || oldGroup.status !== 'completed') {
                            // Animate and delete logic here
                            const groupEl = document.querySelector(`.shared-quest-group[data-shared-group-id="${newGroup.id}"]`);
                            if (groupEl) {
                                groupEl.classList.add('shared-quest-finished');
                                createConfetti(groupEl);
                                groupEl.addEventListener('animationend', async () => {
                                    if (user.uid === newGroup.ownerUid) {
                                        await deleteDoc(doc(db, "sharedGroups", newGroup.id));
                                    }
                                }, { once: true });
                            }
                        }
                    }
                    groupsMap.set(change.doc.id, newGroup);
                }
            });

            allSharedGroupsFromListener = Array.from(groupsMap.values());

            sharedGroups = allSharedGroupsFromListener.filter(g => g.status === 'active' || g.status === 'completed');
            const incomingSharedGroups = allSharedGroupsFromListener.filter(g => g.status === 'pending' && g.friendUid === user.uid);
            
            // Combine with individual quests for the "Shares" tab
            const incomingSharedQuests = sharedQuests.filter(q => q.status === 'pending' && q.friendUid === user.uid);
            incomingSharedItems = [...incomingSharedQuests, ...incomingSharedGroups];

            updateMainNotificationBadges();
            renderAllLists();
        }, (error) => console.error("Error listening for shared groups:", getCoolErrorMessage(error)));
    }

    async function openShareModal(questId) {
        const { task } = findTaskAndContext(questId);
        if (!task) {
            console.error("Task not found for sharing.");
            return;
        }
        if (task.isShared) {
            showConfirm("Already Shared", "This quest has already been shared.", () => {});
            return;
        }

        shareQuestIdInput.value = questId;
        openModal(shareQuestModal);
        await populateFriendListForSharing(shareQuestFriendList);
    }

    shareQuestFriendList.addEventListener('click', async (e) => {
        const button = e.target.closest('.share-btn-action');
        if (button) {
            button.disabled = true;
            button.textContent = 'Sending...';
            const questId = shareQuestIdInput.value;
            const friendUid = button.dataset.uid;
            const friendUsername = button.dataset.username;
            
            if (questId === 'batch_share') {
                const promises = [];
                selectedQuestIds.forEach(id => {
                    const { task } = findTaskAndContext(id);
                    if (task && !task.isShared) {
                        promises.push(shareQuest(id, friendUid, friendUsername));
                    }
                });
                await Promise.all(promises);
                deactivateMultiSelectMode();
                renderAllLists();
            } else {
                try {
                    await shareQuest(questId, friendUid, friendUsername);
                } catch (error) {
                    console.error("Failed to share quest:", error);
                    showConfirm("Sharing Failed", "An error occurred while sharing the quest. Please try again.", () => {});
                    button.disabled = false;
                    button.textContent = 'Share';
                }
            }
            closeModal(shareQuestModal);
        }
    });

    async function shareQuest(questId, friendUid, friendUsername) {
        const { task, list, type, group } = findTaskAndContext(questId);
        if (!task || !list) {
            console.error("Original task not found for sharing.");
            return;
        }
        if (task.isShared) {
            console.warn("Attempted to share an already shared task.");
            return;
        }

        const userDoc = await getDoc(doc(db, "users", user.uid));
        const ownerUsername = userDoc.data().username;
        
        const sharedQuestRef = doc(collection(db, "sharedQuests"));

        const sharedQuestData = {
            text: task.text,
            ownerUid: user.uid,
            ownerUsername: ownerUsername,
            friendUid: friendUid,
            friendUsername: friendUsername,
            ownerCompleted: false,
            friendCompleted: false,
            createdAt: Date.now(),
            participants: [user.uid, friendUid],
            status: 'pending', // NEW: Initial status is pending
            originalTaskId: task.id,
            originalTaskType: type,
            originalGroupId: group ? group.id : null
        };
        
        const batch = writeBatch(db);
        // Step 1: Create the shared quest document with 'pending' status
        batch.set(sharedQuestRef, sharedQuestData);

        // Step 2: Mark the original task in the owner's list as shared
        task.isShared = true;
        task.sharedQuestId = sharedQuestRef.id; // Store the ID of the shared quest document

        // Step 3: Save the updated local state to Firestore
        const dataToSave = { 
            dailyTasks: dailyTasks, 
            standaloneMainQuests: standaloneMainQuests, 
            generalTaskGroups: generalTaskGroups.map(({ isExpanded, ...rest }) => rest),
            playerData: playerData, 
            settings: settings
        };
        batch.set(doc(db, "users", user.uid), { appData: dataToSave }, { merge: true });
        
        await batch.commit();
        audioManager.playSound('share');
        renderAllLists(); // Re-render to show the original task as 'isShared'
    }
    
    async function completeSharedQuestPart(task, uncompleting = false) {
        const questId = task.questId;
        const sharedQuestRef = doc(db, "sharedQuests", questId);
        const isOwner = user && task.ownerUid === user.uid;
        
        const currentSharedQuestSnap = await getDoc(sharedQuestRef);
        if (!currentSharedQuestSnap.exists()) {
            console.error("Shared quest not found:", questId);
            return;
        }
        const currentSharedQuestData = currentSharedQuestSnap.data();

        const updateData = {};
        if (isOwner) {
            // Only proceed if the state is actually changing
            if (!uncompleting && currentSharedQuestData.ownerCompleted) return;
            if (uncompleting && !currentSharedQuestData.ownerCompleted) return;
            updateData.ownerCompleted = !uncompleting;
        } else {
            // Only proceed if the state is actually changing
            if (!uncompleting && currentSharedQuestData.friendCompleted) return;
            if (uncompleting && !currentSharedQuestData.friendCompleted) return;
            updateData.friendCompleted = !uncompleting;
        }
        
        await updateDoc(sharedQuestRef, updateData);
        
        if (!uncompleting) {
            audioManager.playSound('complete');
            addXp(XP_PER_SHARED_QUEST / 2);
        } else {
            audioManager.playSound('delete');
            addXp(-(XP_PER_SHARED_QUEST / 2));
        }

        // Fetch the updated document to check if both are completed
        const updatedDoc = await getDoc(sharedQuestRef);
        if (updatedDoc.exists() && updatedDoc.data().ownerCompleted && updatedDoc.data().friendCompleted) {
            // Now, just update the status. The onSnapshot listener will handle the animation.
            await updateDoc(sharedQuestRef, { status: 'completed' });
        }
    }
    
    async function finishSharedQuestAnimation(questData) {
        audioManager.playSound('sharedQuestFinish');
        const taskEl = document.querySelector(`.task-item[data-id="${questData.id}"]`);
        
        if (taskEl) {
            taskEl.classList.add('shared-quest-finished');
            createConfetti(taskEl);
            taskEl.addEventListener('animationend', async () => {
                // Only the owner deletes the document to prevent race conditions.
                const isOwner = user && questData.ownerUid === user.uid;
                if (isOwner) {
                    const sharedQuestRef = doc(db, "sharedQuests", questData.id);
                    await deleteDoc(sharedQuestRef).catch(err => {
                        if (err.code !== 'not-found') {
                            console.error("Error deleting shared quest:", getCoolErrorMessage(err));
                        }
                    });
                }
            }, { once: true });
        } else {
            // If element not found (e.g., user on different screen), owner still cleans up.
            const isOwner = user && questData.ownerUid === user.uid;
            if (isOwner) {
                const sharedQuestRef = doc(db, "sharedQuests", questData.id);
                await deleteDoc(sharedQuestRef).catch(err => {
                    if (err.code !== 'not-found') {
                        console.error("Error deleting shared quest:", getCoolErrorMessage(err));
                    }
                });
            }
        }
    }

    async function openShareGroupModal(groupId) {
        if (!user) return; 
        const { group } = findTaskAndContext(groupId);
        if (group && group.isShared) {
            showConfirm("Already Shared", "This group has already been shared.", () => {});
            return;
        }

        if (!group || !group.tasks || group.tasks.filter(t => !t.isShared).length === 0) {
            showConfirm("Cannot Share Group", "This group has no non-shared tasks to share.", () => {});
            return;
        }

        shareGroupNameDisplay.textContent = group.name;
        shareGroupIdInput.value = groupId;
        openModal(shareGroupModal);
        await populateFriendListForSharing(shareGroupFriendList);
    }

    shareGroupFriendList.addEventListener('click', async (e) => {
        const button = e.target.closest('.share-btn-action');
        if (button) {
            button.disabled = true;
            button.textContent = 'Sending...';
            const groupId = shareGroupIdInput.value;
            const friendUid = button.dataset.uid;
            const friendUsername = button.dataset.username;

            try {
                await shareGroup(groupId, friendUid, friendUsername);
                closeModal(shareGroupModal);
            } catch (error) {
                console.error("Failed to share group:", error);
                showConfirm("Sharing Failed", "An error occurred while sharing the group. Please try again.", () => {});
                button.disabled = false;
                button.textContent = 'Share';
            }
        }
    });

    async function shareGroup(groupId, friendUid, friendUsername) {
        if (!user) return;

        const groupIndex = generalTaskGroups.findIndex(g => g.id === groupId);
        if (groupIndex === -1) return;

        const groupToShare = generalTaskGroups[groupIndex]; // Get the group by index
        if (groupToShare.isShared) {
            console.warn("Attempted to share an already shared group.");
            return;
        }
        const tasksToShare = groupToShare.tasks.filter(t => !t.isShared);

        if (tasksToShare.length === 0) {
            console.warn("Attempted to share a group with no non-shared tasks.");
            showConfirm("Cannot Share", "This group has no tasks that can be shared.", () => {});
            return;
        }

        const userDoc = await getDoc(doc(db, "users", user.uid));
        const ownerUsername = userDoc.data().username;

        const batch = writeBatch(db);
        const sharedGroupRef = doc(collection(db, "sharedGroups"));

        const sharedTasks = tasksToShare.map(task => ({
            id: task.id,
            text: task.text,
            ownerCompleted: false,
            friendCompleted: false
        }));

        const sharedGroupData = {
            name: groupToShare.name,
            tasks: sharedTasks,
            ownerUid: user.uid,
            ownerUsername: ownerUsername,
            friendUid: friendUid,
            friendUsername: friendUsername,
            createdAt: Date.now(),
            participants: [user.uid, friendUid],
            status: 'pending',
            originalGroupId: groupId // Keep this for reverting
        };

        batch.set(sharedGroupRef, sharedGroupData);

        // Mark the original group as shared, so it renders as a placeholder.
        groupToShare.isShared = true;
        groupToShare.sharedGroupId = sharedGroupRef.id;

        // The saveState() call will persist the updated state of the group.
        saveState();

        await batch.commit();

        audioManager.playSound('share');
        renderAllLists();
    }

    function renderIncomingItems() {
        incomingSharesContainer.innerHTML = '';
        if (incomingSharedItems.length === 0) {
            incomingSharesContainer.innerHTML = `<p style="text-align: center; padding: 1rem;">No incoming shares.</p>`;
            sharesRequestCountBadge.style.display = 'none';
            return;
        }

        const sharesCount = incomingSharedItems.length;
        if (sharesCount > 0) {
            sharesRequestCountBadge.textContent = sharesCount;
            sharesRequestCountBadge.style.display = 'flex';
        } else {
            sharesRequestCountBadge.style.display = 'none';
        }


        incomingSharedItems.forEach(item => {
            const shareItemEl = document.createElement('div');
            shareItemEl.className = 'incoming-share-item';
            
            if (item.tasks) { // It's a group
                shareItemEl.innerHTML = `
                    <span>Group "${item.name}" from ${item.ownerUsername}</span>
                    <div class="incoming-share-actions">
                        <button class="btn icon-btn accept-share-btn" data-item-id="${item.id}" data-type="group" aria-label="Accept shared group">&#10003;</button>
                        <button class="btn icon-btn deny-share-btn" data-item-id="${item.id}" data-type="group" aria-label="Deny shared group">&times;</button>
                    </div>
                `;
            } else { // It's a quest
                shareItemEl.innerHTML = `
                    <span>"${item.text}" from ${item.ownerUsername}</span>
                    <div class="incoming-share-actions">
                        <button class="btn icon-btn accept-share-btn" data-item-id="${item.questId}" data-type="quest" aria-label="Accept shared quest">&#10003;</button>
                        <button class="btn icon-btn deny-share-btn" data-item-id="${item.questId}" data-type="quest" aria-label="Deny shared quest">&times;</button>
                    </div>
                `;
            }
            incomingSharesContainer.appendChild(shareItemEl);
        });
    }

    // NEW: Accept a shared quest
    async function acceptSharedItem(itemId, type) {
        if (!user) return;
        const collectionName = type === 'group' ? 'sharedGroups' : 'sharedQuests';
        const sharedQuestRef = doc(db, collectionName, itemId);
        try {
            await updateDoc(sharedQuestRef, { status: 'active' });
            audioManager.playSound('toggle'); // Use toggle sound for acceptance
        } catch (error) {
            console.error("Error accepting shared quest:", getCoolErrorMessage(error));
            showConfirm("Error", "Failed to accept quest. Please try again.", () => {});
        }
    }

    // NEW: Deny a shared quest
    async function denySharedItem(itemId, type) {
        if (!user) return;
        const collectionName = type === 'group' ? 'sharedGroups' : 'sharedQuests';
        const sharedQuestRef = doc(db, collectionName, itemId);
        const itemTypeName = type === 'group' ? 'Group' : 'Quest';
        showConfirm(`Deny Shared ${itemTypeName}?`, `The owner will be notified and the ${itemTypeName.toLowerCase()} will revert to a normal item for them.`, async () => {
            try {
                // Instead of deleting, update the status to 'rejected'.
                // The owner's client will listen for this change and clean up.
                await updateDoc(sharedQuestRef, { status: 'rejected' });
                audioManager.playSound('delete');
            } catch (error) {
                console.error("Error denying shared quest:", getCoolErrorMessage(error));
                showConfirm("Error", "Failed to deny quest. Please try again.", () => {});
            }
        });
    }

    // NEW: Event listener for accept/deny buttons in the Shares tab
    incomingSharesContainer.addEventListener('click', (e) => {
        const acceptBtn = e.target.closest('.accept-share-btn');
        const denyBtn = e.target.closest('.deny-share-btn');
        
        if (acceptBtn) {
            const itemId = acceptBtn.dataset.itemId;
            const type = acceptBtn.dataset.type;
            acceptSharedItem(itemId, type);
        } else if (denyBtn) {
            const itemId = denyBtn.dataset.itemId;
            const type = denyBtn.dataset.type;
            denySharedItem(itemId, type);
        }
    });

    // --- MULTI-SELECT LOGIC ---

    function updateBatchActionsUI() {
        if (!isMultiSelectModeActive) return;

        const count = selectedQuestIds.size;
        batchActionsModalCounter.textContent = `${count} item${count !== 1 ? 's' : ''} selected`;

        const hasSelection = count > 0;
        let canComplete = hasSelection;
        let canUncomplete = hasSelection;
        let canSetTimer = hasSelection;
        let canShare = hasSelection;
        let canUnshare = false;
        let canDelete = hasSelection;

        if (hasSelection) {
            let hasShared = false;
            let hasUnshared = false;
            let hasGroup = false;
            let hasActiveShared = false;

            for (const id of selectedQuestIds) {
                const { task, group } = findTaskAndContext(id);
                const item = task || group;

                if (!item) {
                    canComplete = canUncomplete = canSetTimer = canShare = canUnshare = canDelete = false;
                    break;
                }

                if (group) hasGroup = true;
                if (item.isShared || (task && task.status)) hasShared = true;
                else hasUnshared = true;
                if (task && task.status === 'active') hasActiveShared = true;
            }

            if (hasShared && hasUnshared) { // Mixed selection
                canShare = false;
                canUnshare = false;
            } else if (hasShared) { // Only shared items
                canShare = false;
                canUnshare = hasActiveShared;
            } else { // Only unshared items
                canUnshare = false;
            }
            
            if (hasGroup) {
                canComplete = false;
                canUncomplete = false;
                canSetTimer = false;
                canUnshare = false;
            }
        }

        batchModalCompleteBtn.disabled = !canComplete;
        batchModalUncompleteBtn.disabled = !canUncomplete;
        batchModalTimerBtn.disabled = !canSetTimer;
        batchModalShareBtn.disabled = !canShare;
        batchModalUnshareBtn.disabled = !canUnshare;
        batchModalDeleteBtn.disabled = !canDelete;
    }

    function deactivateMultiSelectMode() {
        isMultiSelectModeActive = false;
        questsLayout.classList.remove('multi-select-active');
        multiSelectToggleBtns.forEach(btn => btn.classList.remove('active'));
        
        document.querySelectorAll('.multi-select-selected').forEach(el => el.classList.remove('multi-select-selected'));
        selectedQuestIds.clear();

        closeModal(batchActionsModal);
    }

    function enableMultiSelectMode() {
        isMultiSelectModeActive = true;
        questsLayout.classList.add('multi-select-active');
        multiSelectToggleBtns.forEach(btn => btn.classList.add('active'));
        
        selectedQuestIds.clear();
        document.querySelectorAll('.multi-select-selected').forEach(el => el.classList.remove('multi-select-selected'));

        // The modal is not opened automatically.
        // The UI is updated as items are selected.
    }

    function toggleMultiSelectMode() {
        if (!isMultiSelectModeActive) {
            // First click: enable the mode.
            enableMultiSelectMode();
        } else {
            // Mode is already active.
            if (selectedQuestIds.size > 0) {
                // Second click with items selected: open the modal.
                openModal(batchActionsModal);
                updateBatchActionsUI(); // Update counts etc. when opening.
            } else {
                // Second click with no items selected: disable the mode.
                deactivateMultiSelectMode();
            }
        }
    }

    function addBatchActionListeners() {
        batchModalCompleteBtn.addEventListener('click', () => {
            selectedQuestIds.forEach(id => {
                const { task, type } = findTaskAndContext(id);
                if (task && !task.isShared) {
                    if (type === 'daily' && !task.completedToday) {
                        completeTask(id);
                    } else if ((type === 'standalone' || type === 'group') && !task.pendingDeletion) {
                        completeTask(id);
                    }
                }
            });
            deactivateMultiSelectMode();
            renderAllLists();
        });

        batchModalUncompleteBtn.addEventListener('click', () => {
            selectedQuestIds.forEach(id => {
                const { task, type } = findTaskAndContext(id);
                if (task && type === 'daily' && task.completedToday) {
                    uncompleteDailyTask(id);
                }
            });
            deactivateMultiSelectMode();
            renderAllLists();
        });

        batchModalTimerBtn.addEventListener('click', () => {
            currentEditingTaskId = 'batch_timer';
            openModal(timerModal);
        });

        batchModalShareBtn.addEventListener('click', () => {
            if (!user) {
                showConfirm("Login Required", "You must be logged in to share quests.", () => {
                    closeModal(batchActionsModal);
                    openModal(accountModal);
                });
                return;
            }
            shareQuestIdInput.value = 'batch_share';
            openModal(shareQuestModal);
            populateFriendListForSharing(shareQuestFriendList);
        });

        batchModalUnshareBtn.addEventListener('click', () => {
            const selectedCount = selectedQuestIds.size;
            if (selectedCount === 0) return;

            showConfirm(`Unshare/Abandon ${selectedCount} quest${selectedCount > 1 ? 's' : ''}?`, "This will remove the quests from your friends' lists and convert them back to normal quests for the owner.", async () => {
                const promises = [];
                for (const id of selectedQuestIds) {
                    const { task } = findTaskAndContext(id);
                    // Only act on active shared quests selected for unsharing
                    if (task && task.status === 'active') { 
                        if (user.uid === task.ownerUid) {
                            // If I am the owner, I unshare.
                            promises.push(updateDoc(doc(db, "sharedQuests", id), { status: 'unshared' }));
                        } else {
                            // If I am the friend, I abandon.
                            promises.push(updateDoc(doc(db, "sharedQuests", id), { status: 'abandoned' }));
                        }
                    }
                }
                
                try {
                    await Promise.all(promises);
                    audioManager.playSound('delete');
                } catch (error) {
                    console.error("Batch unshare/abandon failed:", getCoolErrorMessage(error));
                    showConfirm("Error", "Could not update all selected quests. Please try again.", () => {});
                }
                deactivateMultiSelectMode();
            });
        });

        batchModalDeleteBtn.addEventListener('click', () => {
            showConfirm(`Delete ${selectedQuestIds.size} items?`, "This action cannot be undone.", () => {
                let needsSave = false;
                selectedQuestIds.forEach(id => {
                    const { task, list, group } = findTaskAndContext(id);
                    if ((task && !task.isShared) || (group && !group.isShared)) {
                        if (task) {
                            stopTimer(id, false);
                            const i = list.findIndex(t => t.id === id);
                            if (i > -1) { list.splice(i, 1); needsSave = true; }
                        } else if (group) {
                            const i = generalTaskGroups.findIndex(g => g.id === id);
                            if (i > -1) { generalTaskGroups.splice(i, 1); needsSave = true; }
                        }
                    }
                });
                
                if (needsSave) saveState();
                deactivateMultiSelectMode();
                renderAllLists();
                audioManager.playSound('delete');
            });
        });

        batchActionsModal.querySelector('[data-close-modal="batch-actions-modal"]').addEventListener('click', deactivateMultiSelectMode);
        batchActionsModal.addEventListener('click', (e) => {
            if (e.target === batchActionsModal) deactivateMultiSelectMode();
        });
    }

    const initOnce = () => {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', applySettings);
        showRandomQuote();
        document.addEventListener('keydown', handleGlobalKeys);

        multiSelectToggleBtns.forEach(btn => btn.addEventListener('click', toggleMultiSelectMode));
        addBatchActionListeners();

        // NEW: Add keydown listener for Shift to show actions immediately
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Shift' && !e.repeat && lastPotentialShiftHoverItem) {
                showShiftHoverActions(lastPotentialShiftHoverItem);
            }
        });

        // Listen for Shift key release to close any hover-opened menus
        document.addEventListener('keyup', (e) => {
            if (e.key === 'Shift' && shiftHoverItem) {
                hideActiveTaskActions();
                shiftHoverItem = null;
            }
        });

        // Add a global click listener to close active task actions when clicking outside.
        document.addEventListener('click', (e) => {
            if (activeMobileActionsItem && !activeMobileActionsItem.contains(e.target)) {
                hideActiveTaskActions();
            }
        });

        // Add online/offline listeners to show the indicator.
        const updateOnlineStatus = () => {
            if (offlineIndicator) { // Defensive check
                offlineIndicator.classList.toggle('visible', !navigator.onLine);
            }
        };
        window.addEventListener('online', updateOnlineStatus);
        window.addEventListener('offline', updateOnlineStatus);
        updateOnlineStatus(); // Check initial status on load
    };

    const handleGlobalKeys = (e) => {
        const activeModal = document.querySelector('.modal-overlay.visible');

        // Escape key to close modals
        if (e.key === 'Escape' && activeModal) {
            if (activeModal.getAttribute('data-persistent') !== 'true') {
                closeModal(activeModal);
            }
        }

        // Tab trapping inside modals
        if (e.key === 'Tab' && activeModal) {
            const focusableElements = Array.from(activeModal.querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'))
                                           .filter(el => el.offsetParent !== null); // Only visible, focusable elements
            if (focusableElements.length === 0) {
                e.preventDefault();
                return;
            }
            const firstFocusable = focusableElements[0];
            const lastFocusable = focusableElements[focusableElements.length - 1];

            if (e.shiftKey) { // Shift + Tab
                if (document.activeElement === firstFocusable) {
                    lastFocusable.focus();
                    e.preventDefault();
                }
            } else { // Tab
                if (document.activeElement === lastFocusable) {
                    firstFocusable.focus();
                    e.preventDefault();
                }
            }
        }
    };

    const showShiftHoverActions = (item) => {
        // Only proceed if we found an item and it's not the one we're already hovering
        if (item && item !== shiftHoverItem) {
            // If a menu is already open from a click (and not from a previous shift-hover), do nothing.
            if (activeMobileActionsItem && activeMobileActionsItem !== shiftHoverItem) {
                return;
            }

            // Hide any previously hover-opened menu.
            if (shiftHoverItem) {
                hideActiveTaskActions(); // This will nullify activeMobileActionsItem
                shiftHoverItem = null;
            }
            
            // --- Pre-condition checks for showing the menu ---
            if (item.classList.contains('timer-active')) return;
            const optionsBtn = item.querySelector('.options-btn');
            if (optionsBtn && optionsBtn.disabled) return;

            // Don't show for placeholder shared items in main/daily lists.
            const isPlaceholder = item.closest('.is-shared-task') && !item.closest('#shared-quests-container');
            if (isPlaceholder) return;

            // Show the new menu
            item.classList.add('actions-visible');
            if (optionsBtn) optionsBtn.classList.add('is-active-trigger');
            activeMobileActionsItem = item; // Use the global state.
            shiftHoverItem = item; // Mark it as hover-opened.
        }
    };

    document.querySelector('.quests-layout').addEventListener('mouseover', (e) => {
        const item = e.target.closest('.task-item, .main-quest-group-header');
        lastPotentialShiftHoverItem = item;

        if (e.shiftKey) {
            showShiftHoverActions(item);
        }
    });

    document.querySelector('.quests-layout').addEventListener('mouseout', (e) => {
        const item = e.target.closest('.task-item, .main-quest-group-header');
        if (item && !item.contains(e.relatedTarget)) {
            lastPotentialShiftHoverItem = null;
            // If we are moving out of the item that has the shift-hover menu open, close it.
            if (shiftHoverItem === item) {
                hideActiveTaskActions();
                shiftHoverItem = null;
            }
        }
    });

    initOnce();
    await loadUserSession();

    return {
        isPartial: false,
        shutdown: () => {
             debouncedSaveData.cancel();
             activeTimers.forEach(timeoutId => clearTimeout(timeoutId));
             activeTimers.clear();
             if (unsubscribeFromFriendsAndShares) unsubscribeFromFriendsAndShares();
             if (unsubscribeFromSharedQuests) unsubscribeFromSharedQuests();
             if (unsubscribeFromSharedGroups) unsubscribeFromSharedGroups();
        },
        updateUser: async (newUser) => {
            user = newUser;
            await loadUserSession();
        }
    };
}

function getCoolErrorMessage(error) {
    const defaultMessage = "An unexpected vortex appeared! Please try again.";
    if (!error) return defaultMessage;
    if (error.message && error.message.toLowerCase().includes("missing or insufficient permissions")) {
        return "Permission Denied! Check your Firestore Security Rules.";
    }
    if (error.code === 'permission-denied') {
         return "Permission Denied! Please check your Firestore Security Rules in the Firebase console.";
    }
    switch (error.code) {
        case 'auth/invalid-email': return "Hmm, that email doesn't look right. Check for typos?";
        case 'auth/user-disabled': return "This account has been disabled. Contact support for help.";
        case 'auth/user-not-found': return "No account found with this email or username. Time to sign up?";
        case 'auth/wrong-password': return "Incorrect password. Did you forget? It happens to the best of us!";
        case 'auth/email-already-in-use': return "An account with this email already exists. Try logging in!";
        case 'auth/weak-password': return "Password should be at least 6 characters long. Make it strong!";
        case 'auth/requires-recent-login': return "This is a sensitive action. Please log in again to continue.";
        case 'auth/popup-closed-by-user': return "Sign-in cancelled. Did you change your mind?";
        case 'auth/account-exists-with-different-credential': return "You've already signed up with this email using a different method (e.g., Google). Try logging in that way!";
        case 'auth/too-many-requests': return "You have made too many sign-in attempts. Please wait a bit before trying again.";
        case 'auth/invalid-credential': return "Invalid login credentials. Please check your username and password.";
        default:
            console.error("Firebase/App Error:", error);
            return "An unexpected error occurred. Check the console for more details.";
    }
}

function setupAuthForms(container, onAuthSuccess) {
    container.innerHTML = '';
    const template = document.getElementById('account-modal-content');
    const content = template.content.cloneNode(true);
    container.appendChild(content);

    const toggleBtns = container.querySelectorAll('.toggle-btn');
    const signupForm = container.querySelector('[data-form="signup"]');
    const loginForm = container.querySelector('[data-form="login"]');
    const googleBtnContainer = container.querySelector('.google-signin-btn-container');

    const googleBtn = document.createElement('button');
    googleBtn.type = 'button';
    googleBtn.className = 'google-btn-custom';
    googleBtn.innerHTML = `<svg viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 6.93l3.66 2.84c.87-2.6 3.3-4.39 6.16-4.39z"/><path fill="none" d="M1 1h22v22H1z"/></svg><span>Sign in with Google</span>`;
    if(googleBtnContainer) googleBtnContainer.appendChild(googleBtn);
    
    toggleBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            toggleBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const showSignup = btn.dataset.tab === 'signup';
            signupForm.style.display = showSignup ? 'block' : 'none';
            loginForm.style.display = showSignup ? 'none' : 'block';
        });
    });

    signupForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = signupForm.querySelector('.signup-username').value.trim().toLowerCase();
        const email = signupForm.querySelector('.signup-email').value;
        const password = signupForm.querySelector('.signup-password').value;
        const errorEl = signupForm.querySelector('.signup-error');
        errorEl.textContent = '';

        try {
            const usernamesRef = doc(db, "usernames", username);
            const usernameSnap = await getDoc(usernamesRef);
            if (usernameSnap.exists()) {
                throw new Error('This username is already taken.');
            }

            await createUserWithEmailAndPassword(auth, email, password);

            onAuthSuccess();
        } catch (error) {
            errorEl.textContent = error.message || getCoolErrorMessage(error);
        }
    });

    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const emailOrUsername = loginForm.querySelector('.login-email').value;
        const password = loginForm.querySelector('.login-password').value;
        const errorEl = loginForm.querySelector('.login-error');
        errorEl.textContent = '';
        
        try {
            let emailToLogin = emailOrUsername;
            if (!emailOrUsername.includes('@')) {
                const usernamesRef = doc(db, "usernames", emailOrUsername.toLowerCase());
                const usernameSnap = await getDoc(usernamesRef);
                if (usernameSnap.exists()) {
                    const targetUserId = usernameSnap.data().userId;
                    const userDocRef = doc(db, "users", targetUserId);
                    const userDocSnap = await getDoc(userDocRef);
                    if(userDocSnap.exists()) {
                       emailToLogin = userDocSnap.data().email;
                    } else { throw new Error("User data not found for this username."); }
                } else { throw { code: 'auth/user-not-found' }; }
            }
            await signInWithEmailAndPassword(auth, emailToLogin, password);
            onAuthSuccess();
        } catch (error) {
            errorEl.textContent = getCoolErrorMessage(error);
        }
    });

    if(googleBtn) {
        googleBtn.addEventListener('click', async () => {
            const provider = new GoogleAuthProvider();
            const googleLoader = document.getElementById('google-signin-loader-modal');
            try {
                openModal(googleLoader);
                await signInWithPopup(auth, provider);
                onAuthSuccess();
            } catch (error) {
                console.error("Google Sign-In Error: ", error);
                const errorEl = loginForm.querySelector('.login-error');
                errorEl.textContent = getCoolErrorMessage(error);
            } finally {
                closeModal(googleLoader);
            }
        });
    }
}

function mergeGuestDataWithCloud(cloudData = {}) {
    const guestDataString = localStorage.getItem('anonymousUserData');
    if (!guestDataString) return cloudData;
    try {
        const guestData = JSON.parse(guestDataString);
        const mergedData = JSON.parse(JSON.stringify(cloudData));
        const mergeTasks = (cloudTasks = [], guestTasks = []) => {
            const existingTexts = new Set(cloudTasks.map(t => t.text));
            const newTasks = guestTasks.filter(t => !existingTexts.has(t.text));
            return [...cloudTasks, ...newTasks];
        };
        mergedData.dailyTasks = mergeTasks(cloudData.dailyTasks, guestData.dailyTasks);
        mergedData.standaloneMainQuests = mergeTasks(cloudData.standaloneMainQuests, guestData.standaloneMainQuests);
        if (guestData.generalTaskGroups) {
            if (!mergedData.generalTaskGroups) mergedData.generalTaskGroups = [];
            guestData.generalTaskGroups.forEach(guestGroup => {
                const cloudGroup = mergedData.generalTaskGroups.find(cg => cg.name === guestGroup.name);
                if (cloudGroup) {
                    cloudGroup.tasks = mergeTasks(cloudGroup.tasks, guestGroup.tasks);
                } else {
                    mergedData.generalTaskGroups.push(guestGroup);
                }
            });
        }
        if (guestData.playerData) {
            if (!mergedData.playerData) {
                mergedData.playerData = guestData.playerData;
            } else {
                const newXp = (mergedData.playerData.xp || 0) + (guestData.playerData.xp || 0);
                const newLevel = Math.max(mergedData.playerData.level || 1, guestData.playerData.level || 1);
                mergedData.playerData.xp = newXp;
                mergedData.playerData.level = newLevel;
            }
        }
        return mergedData;
    } catch (error) {
        console.error("Failed to merge guest data:", error);
        return cloudData;
    }
}