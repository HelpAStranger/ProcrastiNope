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
    documentId
} from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";


// --- NOTE ON SECURITY & PERMISSIONS ERRORS ---
// This update adds a new 'sharedQuests' collection. To enable this
// feature, you MUST add the new rules section to your
// Firestore Security Rules in the Firebase Console.
//
// ==> Copy and paste the following rules into your Firebase Console <==
// ==> (Firestore Database -> Rules tab)                               <==
//
// rules_version = '2';
// service cloud.firestore {
//   match /databases/{database}/documents {
//
//     // Usernames must be unique and are public.
//     match /usernames/{username} {
//       allow read;
//       allow create: if request.auth != null && request.resource.data.userId == request.auth.uid;
//       allow delete: if request.auth != null && resource.data.userId == request.auth.uid;
//     }
//
//     // User data collection.
//     match /users/{userId} {
//       allow read: if request.auth != null;
//       allow create: if request.auth != null && request.auth.uid == userId;
//       allow update: if request.auth != null;
//       allow delete: if request.auth != null && request.auth.uid == userId;
//     }
//
//     // NEW: Rules for shared quests collection
//     match /sharedQuests/{questId} {
//       // Only the two users involved in the quest can interact with it.
//       allow read, update, delete: if request.auth != null && 
//                                     (request.auth.uid == resource.data.ownerUid || 
//                                      request.auth.uid == resource.data.friendUid);
//       // The owner is the one who creates the shared quest document.
//       allow create: if request.auth != null && 
//                       request.auth.uid == request.resource.data.ownerUid;
//     }
//   }
// }
//
// Your Firebase config is meant to be public. True security is enforced
// by your Firestore Security Rules, not by hiding your API keys.

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
let unsubscribeFromFriends = null;
let unsubscribeFromSharedQuests = null; 
let appController = null;

let activeMobileActionsItem = null; 

// --- DOM ELEMENTS FOR STARTUP ---
const loaderOverlay = document.getElementById('loader-overlay');
const landingPage = document.getElementById('landing-page');
const appWrapper = document.getElementById('app-wrapper');
const landingChoices = document.getElementById('landing-choices');
const landingAuthContainer = document.getElementById('landing-auth-container');

// --- GLOBAL HELPER FUNCTIONS & STATE ---
let settings = { theme: 'system', accentColor: 'var(--accent-red)', volume: 0.3 };
let audioCtx = null; // Will be initialized by the app logic

function playSound(type) {
    if (!audioCtx || settings.volume === 0) return;
    const o = audioCtx.createOscillator(), g = audioCtx.createGain();
    o.connect(g); g.connect(audioCtx.destination);
    let v = settings.volume, d = 0.2;
    switch (type) {
        case 'complete': o.type = 'sine'; o.frequency.setValueAtTime(440, audioCtx.currentTime); o.frequency.exponentialRampToValueAtTime(880, audioCtx.currentTime + 0.2); break;
        case 'levelUp': o.type = 'sawtooth'; o.frequency.setValueAtTime(200, audioCtx.currentTime); o.frequency.exponentialRampToValueAtTime(1000, audioCtx.currentTime + 0.4); d = 0.4; v *= 1.2; break;
        case 'timerUp': o.type = 'square'; o.frequency.setValueAtTime(880, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(440, audioCtx.currentTime + 0.5); d = 0.5; break;
        case 'add': case 'addGroup': o.type = 'triangle'; o.frequency.setValueAtTime(300, audioCtx.currentTime); o.frequency.exponentialRampToValueAtTime(600, audioCtx.currentTime + 0.1); d = 0.15; break;
        case 'delete': o.type = 'square'; o.frequency.setValueAtTime(200, audioCtx.currentTime); o.frequency.exponentialRampToValueAtTime(100, audioCtx.currentTime + 0.1); break;
        case 'hover': o.type = 'sine'; o.frequency.setValueAtTime(800, audioCtx.currentTime); v *= 0.2; d = 0.05; break;
        case 'toggle': o.type = 'sawtooth'; o.frequency.setValueAtTime(200, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(400, audioCtx.currentTime + 0.1); d = 0.1; break;
        case 'open': o.type = 'triangle'; o.frequency.setValueAtTime(250, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(500, audioCtx.currentTime + 0.1); break;
        case 'close': o.type = 'triangle'; o.frequency.setValueAtTime(500, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(250, audioCtx.currentTime + 0.1); break;
        case 'share': o.type = 'sine'; o.frequency.setValueAtTime(523.25, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(659.25, audioCtx.currentTime + 0.15); d=0.2; break;
        case 'friendComplete': o.type = 'triangle'; o.frequency.setValueAtTime(659.25, audioCtx.currentTime); o.frequency.linearRampToValueAtTime(880, audioCtx.currentTime + 0.2); d=0.25; v *= 0.8; break;
        case 'sharedQuestFinish': o.type = 'sawtooth'; o.frequency.setValueAtTime(500, audioCtx.currentTime); o.frequency.exponentialRampToValueAtTime(1200, audioCtx.currentTime + 0.5); d = 0.6; v *= 1.3; break;
    }
    g.gain.setValueAtTime(0, audioCtx.currentTime); g.gain.linearRampToValueAtTime(v, audioCtx.currentTime + 0.01);
    o.start(audioCtx.currentTime); g.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + d); o.stop(audioCtx.currentTime + d);
}

const openModal = (modal) => {
    if(modal) {
        if (activeMobileActionsItem) {
            activeMobileActionsItem.classList.remove('actions-visible');
            activeMobileActionsItem = null;
        }
        appWrapper.classList.add('blur-background');
        modal.classList.add('visible');
        playSound('open');
    }
};
const closeModal = (modal) => {
    if(modal) {
        appWrapper.classList.remove('blur-background');
        modal.classList.remove('visible');
        playSound('close');
    }
};

// --- Initialize Firebase and start the auth flow ---
try {
    app = initializeApp(firebaseConfig);
    auth = getAuth(app);
    db = getFirestore(app);
    
    onAuthStateChanged(auth, async (user) => {
        // Cleanup previous user's data listeners to prevent memory leaks.
        if (unsubscribeFromFirestore) {
            unsubscribeFromFirestore(); 
            unsubscribeFromFirestore = null;
        }
        if (unsubscribeFromFriends) {
            unsubscribeFromFriends();
            unsubscribeFromFriends = null;
        }
        if (unsubscribeFromSharedQuests) {
            unsubscribeFromSharedQuests();
            unsubscribeFromSharedQuests = null;
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
        } else { 
            // No user is logged in.
            if (sessionStorage.getItem('isGuest')) {
                loaderOverlay.style.display = 'none';
                landingPage.style.display = 'none';
                appWrapper.style.display = 'block';
                if (!appController) appController = await initializeAppLogic(null);
            } else {
                loaderOverlay.style.display = 'none';
                landingPage.style.display = 'flex';
                appWrapper.style.display = 'none';
                if(appController) appController.shutdown();
                appController = null;
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
        if (!window.matchMedia('(pointer: coarse)').matches && el) {
            el.focus();
        }
    };

    let user = initialUser;
    audioCtx = window.AudioContext ? new AudioContext() : null;
    
    let lastSection = 'daily';
    
    let dailyTasks = [], standaloneMainQuests = [], generalTaskGroups = [], sharedQuests = [];
    let playerData = { level: 1, xp: 0 };
    let currentListToAdd = null, currentEditingTaskId = null;
    const activeTimers = {};
    let actionsTimeoutId = null;
    
    const sharedQuestsContainer = document.getElementById('shared-quests-container');
    const dailyTaskListContainer = document.getElementById('daily-task-list');
    const standaloneTaskListContainer = document.getElementById('standalone-task-list');
    const generalTaskListContainer = document.getElementById('general-task-list-container');
    const playerLevelEl = document.getElementById('player-level');
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
    const dataManagementHeading = document.getElementById('data-management-heading');
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

    const shareQuestModal = document.getElementById('share-quest-modal');
    const shareQuestFriendList = document.getElementById('share-quest-friend-list');
    const shareQuestIdInput = document.getElementById('share-quest-id-input');


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
    
    function debounce(func, delay) {
        let timeout;
        return function(...args) {
            const context = this;
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(context, args), delay);
        };
    }

    async function saveData(data) {
        if (data.settings && data.settings.theme) {
            localStorage.setItem('userTheme', data.settings.theme);
        }

        if (!user) {
            localStorage.setItem('anonymousUserData', JSON.stringify(data));
            return;
        }
        
        try {
            const userDocRef = doc(db, "users", user.uid);
            await setDoc(userDocRef, { appData: data }, { merge: true });
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
            
            listenForFriendRequests();
            listenForSharedQuests();

            const userDocRef = doc(db, "users", user.uid);
            let isFirstLoad = true;
            unsubscribeFromFirestore = onSnapshot(userDocRef, (docSnap) => {
                if (docSnap.exists() && docSnap.data().appData) {
                    loadAndDisplayData(docSnap.data().appData);
                } else {
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

            const userDocRef = doc(db, "users", user.uid);
            const docSnap = await getDoc(userDocRef);
            const username = docSnap.exists() && docSnap.data().username ? docSnap.data().username : user.email;

            userDisplay.textContent = `Logged in as: ${username}`;
            userDisplay.style.display = 'flex';
            dataManagementHeading.textContent = "Cloud Data";
            exportDataBtn.style.display = 'none';
            importDataBtn.style.display = 'none';
            resetProgressBtn.textContent = 'Reset Cloud Data';
            
            mobileNav.querySelector('[data-section="friends"]').style.display = 'flex';

        } else {
            settingsLoginBtn.style.display = 'inline-flex';
            logoutBtn.style.display = 'none';
            manageAccountBtn.style.display = 'none';
            userDisplay.textContent = 'Playing as Guest';
            userDisplay.style.display = 'flex';
            dataManagementHeading.textContent = "Guest Data (Local)";
            exportDataBtn.style.display = 'inline-flex';
            importDataBtn.style.display = 'inline-flex';
            resetProgressBtn.textContent = 'Reset Progress';
            
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
                if(task.shared) return; // Don't reset shared tasks automatically
                if (task.completedToday && task.lastCompleted === yesterday) task.streak = (task.streak || 0) + 1;
                else if (!task.completedToday) task.streak = 0;
                task.completedToday = false;
                delete task.timerFinished; // Clear timer finished state on reset
            });
            localStorage.setItem('lastVisitDate', today);
            saveState();
        }
    };
    function addXp(amount) {
        playerData.xp += Math.round(amount);
        if (playerData.xp < 0) playerData.xp = 0;
        const requiredXp = getXpForNextLevel(playerData.level);
        if (playerData.xp >= requiredXp) levelUp(requiredXp);
        updateProgressUI();
    }
    function levelUp(requiredXp) {
        playerData.level++;
        playerData.xp -= requiredXp;
        playSound('levelUp');
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
    function checkOverdueTasks() {
        const now = Date.now();
        [...dailyTasks, ...standaloneMainQuests, ...generalTaskGroups.flatMap(g => g.tasks)].forEach(task => {
            const taskEl = document.querySelector(`.task-item[data-id="${task.id}"]`);
            if (!taskEl || task.completedToday || task.shared) return;
            taskEl.classList.toggle('overdue', (now - task.createdAt) > 86400000);
        });
    }
    function checkAllTasksCompleted() {
        const allDailiesDone = dailyTasks.length > 0 && dailyTasks.every(t => t.completedToday || t.shared);
        const noStandaloneQuests = standaloneMainQuests.length === 0;
        const noGroupedQuests = generalTaskGroups.every(g => !g.tasks || g.tasks.length === 0);
        return { allDailiesDone, allTasksDone: allDailiesDone && noStandaloneQuests && noGroupedQuests };
    }
    
    const renderSharedQuests = () => { sharedQuestsContainer.innerHTML = ''; sharedQuests.forEach(task => sharedQuestsContainer.appendChild(createTaskElement(task, 'shared'))); };
    const renderDailyTasks = () => { dailyTaskListContainer.innerHTML = ''; noDailyTasksMessage.style.display = dailyTasks.length === 0 ? 'block' : 'none'; dailyTasks.forEach(task => dailyTaskListContainer.appendChild(createTaskElement(task, 'daily'))); };
    const renderStandaloneTasks = () => { standaloneTaskListContainer.innerHTML = ''; standaloneMainQuests.forEach(task => standaloneTaskListContainer.appendChild(createTaskElement(task, 'standalone'))); };
    const renderGeneralTasks = () => { generalTaskListContainer.innerHTML = ''; generalTaskGroups.forEach(group => generalTaskListContainer.appendChild(createGroupElement(group))); noGeneralTasksMessage.style.display = (standaloneMainQuests.length === 0 && generalTaskGroups.length === 0) ? 'block' : 'none'; };
    const createGroupElement = (group) => {
        const el = document.createElement('div'); el.className = 'main-quest-group'; if (group.isExpanded) el.classList.add('expanded'); el.dataset.groupId = group.id;
        el.innerHTML = `<header class="main-quest-group-header"><div class="group-title-container"><div class="expand-icon-wrapper"><svg class="expand-icon" viewBox="0 0 24 24" fill="currentColor"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18L8.59,16.58Z"/></svg></div><h3>${group.name}</h3></div><div class="group-actions"><button class="btn icon-btn edit-group-btn" aria-label="Edit group name"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button><button class="btn icon-btn delete-group-btn" aria-label="Delete group"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button><button class="btn add-task-to-group-btn" aria-label="Add task">+</button></div></header><ul class="task-list-group" data-group-id="${group.id}"></ul>`;
        const ul = el.querySelector('ul'); if (group.tasks) group.tasks.forEach(task => ul.appendChild(createTaskElement(task, 'group'))); return el;
    };
    const createTaskElement = (task, type) => {
        const li = document.createElement('li'); li.className = 'task-item'; li.dataset.id = task.id; if (type === 'standalone') li.classList.add('standalone-quest');
        
        // Shared Quest specific rendering
        if(type === 'shared') {
            li.classList.add('shared-quest');
            li.dataset.id = task.questId;
            
            const isOwner = user && task.ownerUid === user.uid;
            const ownerCompleted = task.ownerCompleted;
            const friendCompleted = task.friendCompleted;
            const otherPlayerUsername = isOwner ? task.friendUsername : task.ownerUsername;

            const selfIdentifier = isOwner ? 'You' : otherPlayerUsername;
            const otherIdentifier = isOwner ? otherPlayerUsername : 'Owner';
            
            li.innerHTML = `
                <button class="complete-btn"></button>
                <div class="task-content"><span class="task-text">${task.text}</span></div>
                <div class="shared-quest-info">
                    <span class="shared-with-tag">with ${otherPlayerUsername}</span>
                    <div class="shared-status-indicators" title="${selfIdentifier} | ${otherIdentifier}">
                        <div class="status-indicator ${ownerCompleted ? 'completed' : ''}"></div>
                        <div class="status-indicator ${friendCompleted ? 'completed' : ''}"></div>
                    </div>
                </div>`;

            const myPartCompleted = isOwner ? ownerCompleted : friendCompleted;
            if(myPartCompleted) {
                 li.querySelector('.complete-btn').classList.add('checked');
                 li.classList.add('daily-completed');
            }
            return li;
        }

        // Regular task rendering
        let streakHTML = ''; if (type === 'daily' && task.streak > 0) streakHTML = `<div class="streak-counter" title="Streak: ${task.streak}"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M17.653 9.473c.071.321.11.65.11.986 0 2.21-1.791 4-4 4s-4-1.79-4-4c0-.336.039-.665.11-.986C7.333 11.23 6 14.331 6 18h12c0-3.669-1.333-6.77-3.347-8.527zM12 2C9.239 2 7 4.239 7 7c0 .961.261 1.861.713 2.638C9.223 8.36 10.55 7.5 12 7.5s2.777.86 4.287 2.138C17 8.861 17 7.961 17 7c0-2.761-2.239-5-5-5z"/></svg><span>${task.streak}</span></div>`;
        let goalHTML = ''; if (type === 'daily' && task.weeklyGoal > 0) { goalHTML = `<div class="weekly-goal-tag" title="Weekly goal"><span>${task.weeklyCompletions}/${task.weeklyGoal}</span></div>`; if (task.weeklyCompletions >= task.weeklyGoal) li.classList.add('weekly-goal-met'); }
        li.innerHTML = `<button class="complete-btn"></button>
            <div class="task-content">${streakHTML}<span class="task-text">${task.text}</span>${goalHTML}</div>
            <div class="task-buttons-wrapper">
                <button class="btn icon-btn timer-clock-btn" aria-label="Set Timer"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><svg class="progress-ring" viewBox="0 0 24 24"><circle class="progress-ring-circle" r="10" cx="12" cy="12"/></svg></button>
                <button class="btn icon-btn share-btn" aria-label="Share Quest"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"></path><polyline points="16 6 12 2 8 6"></polyline><line x1="12" y1="2" x2="12" y2="15"></line></svg></button>
                <button class="btn icon-btn edit-btn" aria-label="Edit Quest"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/></svg></button>
                <button class="btn icon-btn delete-btn" aria-label="Delete Quest"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>
            </div>`;
        if (task.completedToday) { li.classList.add('daily-completed'); li.querySelector('.complete-btn').classList.add('checked'); }
        if (task.timerFinished) li.classList.add('timer-finished');
        if (task.timerStartTime && task.timerDuration) {
            const elapsed = (Date.now() - task.timerStartTime) / 1000;
            const remaining = Math.max(0, task.timerDuration - elapsed);
            if (remaining > 0) {
                li.classList.add('timer-active');
                const ring = li.querySelector('.progress-ring-circle');
                if (ring) {
                    const r = 10;
                    const c = r * 2 * Math.PI;
                    const p = remaining / task.timerDuration;
                    ring.style.strokeDashoffset = c - (p * c);
                }
            }
        }
        return li;
    };
    
    const addTask = (text, list, goal) => {
        const common = { id: Date.now().toString(), text, createdAt: Date.now() };
        if (list === 'daily') dailyTasks.push({ ...common, completedToday: false, lastCompleted: null, streak: 0, weeklyGoal: goal || 0, weeklyCompletions: 0, weekStartDate: getStartOfWeek(new Date()) });
        else if (list === 'standalone') standaloneMainQuests.push({ ...common });
        else { const g = generalTaskGroups.find(g => g.id === list); if (g) { if (!g.tasks) g.tasks = []; g.tasks.push({ ...common }); } }
        renderAllLists();
        saveState(); 
        playSound('add');
    };
    const addGroup = (name) => { 
        generalTaskGroups.push({ id: 'group_' + Date.now(), name, tasks: [], isExpanded: false }); 
        renderAllLists();
        saveState(); 
        playSound('addGroup'); 
    };
    const deleteGroup = (id) => { const name = generalTaskGroups.find(g => g.id === id)?.name || 'this group'; showConfirm(`Delete "${name}"?`, 'All tasks will be deleted.', () => { generalTaskGroups = generalTaskGroups.filter(g => g.id !== id); renderAllLists(); saveState(); playSound('delete'); }); };
    const findTaskAndContext = (id) => {
        let task = dailyTasks.find(t => t && t.id === id); if (task) return { task, list: dailyTasks, type: 'daily' };
        task = standaloneMainQuests.find(t => t && t.id === id); if(task) return { task, list: standaloneMainQuests, type: 'standalone'};
        for (const g of generalTaskGroups) { if (g && g.tasks) { const i = g.tasks.findIndex(t => t && t.id === id); if (i !== -1) return { task: g.tasks[i], list: g.tasks, group: g, type: 'group' }; } } 
        task = sharedQuests.find(t => t && t.questId === id); if (task) return { task, list: sharedQuests, type: 'shared' };
        return {};
    };
    const deleteTask = (id) => { stopTimer(id, false); const {list} = findTaskAndContext(id); if (list) { const i = list.findIndex(t => t.id === id); if(i > -1) list.splice(i, 1); } renderAllLists(); saveState(); playSound('delete'); };
    const completeTask = (id) => {
        stopTimer(id, false);
        const { task, type } = findTaskAndContext(id);
        if (!task) return;

        if(type === 'shared') {
            completeSharedQuestPart(task);
            return;
        }

        addXp(XP_PER_TASK);
        playSound('complete');
        if (type === 'daily') {
            if (task.completedToday) return;
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
            createConfetti(document.querySelector(`.task-item[data-id="${id}"]`));
            const { list, group } = findTaskAndContext(id);
            if (list) {
                const i = list.findIndex(t => t.id === id);
                if (i > -1) list.splice(i, 1);
            }
            if (group && (!group.tasks || group.tasks.length === 0)) {
                const i = generalTaskGroups.findIndex(g => g.id === group.id);
                if (i > -1) generalTaskGroups.splice(i, 1);
            }
        }
        saveState();
        renderAllLists();
        const { allDailiesDone, allTasksDone } = checkAllTasksCompleted();
        if (allTasksDone) createFullScreenConfetti(true);
        else if (allDailiesDone) createFullScreenConfetti(false);
    };
    const uncompleteDailyTask = (id) => {
        const { task, type } = findTaskAndContext(id);
        if (task && (type === 'shared' || task.completedToday)) {
            
            if(type === 'shared') { // Un-complete shared task part
                completeSharedQuestPart(task, true); // `true` to un-complete
                return;
            }

            task.completedToday = false;
            delete task.timerFinished;
            if (task.weeklyGoal > 0 && task.lastCompleted === new Date().toDateString()) {
                task.weeklyCompletions = Math.max(0, (task.weeklyCompletions || 0) - 1);
            }
            addXp(-XP_PER_TASK);
            playSound('delete');
            saveState();
            renderAllLists();
        }
    };
    const editTask = (id, text, goal) => {
        const { task, type } = findTaskAndContext(id);
        if (task) {
            task.text = text;
            if (type === 'daily') task.weeklyGoal = goal;
            saveState();
            renderAllLists();
        }
    };
    function finishTimer(id) {
        playSound('timerUp');
        
        if (activeTimers[id]) {
            clearInterval(activeTimers[id]);
            delete activeTimers[id];
        }

        const { task } = findTaskAndContext(id);
        if (task) {
            task.timerFinished = true;
            delete task.timerStartTime;
            delete task.timerDuration;
            saveState();
            renderAllLists();
        }
    }
    function startTimer(id, mins) {
        stopTimer(id, false);
        const { task } = findTaskAndContext(id);
        if (!task) return;

        task.timerStartTime = Date.now();
        task.timerDuration = mins * 60;
        delete task.timerFinished;
        
        saveState();
        renderAllLists();
    }
    function stopTimer(id, shouldRender = true) {
        if (activeTimers[id]) {
            clearInterval(activeTimers[id]);
            delete activeTimers[id];
        }
        const { task } = findTaskAndContext(id);
        if (task) {
            delete task.timerStartTime;
            delete task.timerDuration;
            delete task.timerFinished; // Also remove the finished flag
            if (shouldRender) {
                saveState();
                renderAllLists();
            }
        }
    }
    function resumeTimers() {
        Object.keys(activeTimers).forEach(id => clearInterval(activeTimers[id]));
        let needsSaveAndRender = false;
        [...dailyTasks, ...standaloneMainQuests, ...generalTaskGroups.flatMap(g => g.tasks || [])].forEach(t => {
            if (t && t.timerStartTime && t.timerDuration) {
                const elapsed = (Date.now() - t.timerStartTime) / 1000;
                const remainingSeconds = t.timerDuration - elapsed;
                
                if (remainingSeconds > 0) {
                     activeTimers[t.id] = setInterval(() => {
                        const currentElapsed = (Date.now() - (t.timerStartTime || 0)) / 1000;
                        const currentRemaining = (t.timerDuration || 0) - currentElapsed;
                        
                        const taskEl = document.querySelector(`.task-item[data-id="${t.id}"]`);
                        if (!taskEl || !activeTimers[t.id]) {
                            clearInterval(activeTimers[t.id]);
                            delete activeTimers[t.id];
                            return;
                        }

                        if (currentRemaining > 0) {
                            const ring = taskEl.querySelector('.progress-ring-circle');
                            if (ring) {
                                const r = ring.r.baseVal.value;
                                if (r > 0) {
                                    const c = r * 2 * Math.PI;
                                    const p = currentRemaining / t.timerDuration;
                                    ring.style.strokeDashoffset = c - (p * c);
                                }
                            }
                        } else {
                            finishTimer(t.id);
                        }
                    }, 1000);
                } else {
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
    }
    
    /**
     * Toggles the visibility of action buttons for a given element (task item or group header).
     * Manages a global `activeMobileActionsItem` to ensure only one set of actions is visible at a time.
     * Includes a timeout to automatically hide actions if no further interaction.
     * @param {HTMLElement} element The DOM element (task-item or main-quest-group-header) whose actions to toggle.
     */
    function toggleTaskActions(element) {
        // If the element has an active timer, its actions are always visible and laid out differently.
        // Do not toggle the overlay in this case.
        if (element.classList.contains('timer-active')) {
            return;
        }

        clearTimeout(actionsTimeoutId); // Clear any existing timeout for any element

        // If another element's actions are visible, hide them
        if (activeMobileActionsItem && activeMobileActionsItem !== element) {
            activeMobileActionsItem.classList.remove('actions-visible');
        }
        
        const wasVisible = element.classList.contains('actions-visible');
        element.classList.toggle('actions-visible');

        if (!wasVisible) { // If it just became visible
            activeMobileActionsItem = element;
            // Set a timeout to hide it after 3 seconds
            actionsTimeoutId = setTimeout(() => {
                if(element.classList.contains('actions-visible')) {
                    element.classList.remove('actions-visible');
                    activeMobileActionsItem = null;
                }
            }, 3000);
        } else { // If it just became hidden
            activeMobileActionsItem = null;
        }
    }

    document.querySelector('.quests-layout').addEventListener('click', (e) => {
        const taskItem = e.target.closest('.task-item');
        const groupHeader = e.target.closest('.main-quest-group-header');

        // Removed handleGroupHeaderMobileActions as it's no longer needed
        
        if (groupHeader) { 
            const groupId = groupHeader.parentElement.dataset.groupId;
            const g = generalTaskGroups.find(g => g.id === groupId);

            const isExpandClick = e.target.closest('.expand-icon-wrapper');
            const isAddClick = e.target.closest('.add-task-to-group-btn');
            const isDeleteClick = e.target.closest('.delete-group-btn');
            const isEditClick = e.target.closest('.edit-group-btn'); // Added for consistency

            if (isExpandClick) {
                if (g) {
                    g.isExpanded = !g.isExpanded; 
                    groupHeader.parentElement.classList.toggle('expanded', g.isExpanded);
                }
                // If actions were visible, keep them visible after expand/collapse
                // and reset the timeout.
                if (groupHeader.classList.contains('actions-visible')) {
                    clearTimeout(actionsTimeoutId); // Reset timeout
                    actionsTimeoutId = setTimeout(() => {
                        if(groupHeader.classList.contains('actions-visible')) {
                            groupHeader.classList.remove('actions-visible');
                            activeMobileActionsItem = null;
                        }
                    }, 3000);
                }
                return;
            }

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
                // If there's an edit group modal, open it here.
                // For now, just let the click fall through to toggleTaskActions.
            }
            
            // If the click was not on any specific button (expand, add, delete, edit),
            // toggle the actions overlay.
            if (!e.target.closest('button')) {
                toggleTaskActions(groupHeader);
            }
            return; 
        }

        if (taskItem) {
            const id = taskItem.dataset.id;
            const { task, type } = findTaskAndContext(id);

            const isMyPartCompleted = () => {
                if(!task) return false;
                if(type !== 'shared') return task.completedToday;

                const isOwner = user && task.ownerUid === user.uid;
                return isOwner ? task.ownerCompleted : task.friendCompleted;
            }
            
            // If the task is completed (or my part of a shared task is completed),
            // a click should uncomplete it, not open the actions menu.
            if (isMyPartCompleted()) {
                uncompleteDailyTask(id);
                return;
            }
            
            // If the click is on a button, perform the button action.
            // Otherwise, toggle the task actions menu.
            if(e.target.closest('button')) {
                currentEditingTaskId = id;
                if (e.target.closest('.complete-btn')) completeTask(id);
                else if (e.target.closest('.delete-btn')) deleteTask(id);
                else if (e.target.closest('.share-btn')) openShareModal(id);
                else if (e.target.closest('.timer-clock-btn')) { const { task } = findTaskAndContext(id); if (task && task.timerStartTime) openModal(timerMenuModal); else openModal(timerModal); }
                else if (e.target.closest('.edit-btn')) {
                    if (task) {
                        editTaskIdInput.value = task.id;
                        editTaskInput.value = task.text;
                        editTaskModal.querySelector('#edit-task-modal-title').textContent = (type === 'daily') ? 'Edit Daily Quest' : 'Edit Main Quest';
                        if (type === 'daily') {
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
                }
            } else {
                // If not a button click and not completed, toggle the actions
                toggleTaskActions(taskItem);
            }
        } 
    });

    addTaskForm.addEventListener('submit', (e) => { e.preventDefault(); const t = newTaskInput.value.trim(); if (t && currentListToAdd) { const goal = (currentListToAdd === 'daily') ? parseInt(weeklyGoalSlider.value, 10) : 0; addTask(t, currentListToAdd, goal); newTaskInput.value = ''; weeklyGoalSlider.value = 0; updateGoalDisplay(weeklyGoalSlider, weeklyGoalDisplay); closeModal(addTaskModal); } });
    editTaskForm.addEventListener('submit', (e) => { e.preventDefault(); const id = editTaskIdInput.value; const newText = editTaskInput.value.trim(); const newGoal = parseInt(editWeeklyGoalSlider.value, 10) || 0; if(id && newText) { editTask(id, newText, newGoal); closeModal(editTaskModal); } });
    timerForm.addEventListener('submit', (e) => { e.preventDefault(); const v = parseInt(timerDurationSlider.value,10), u = timerUnitSelector.querySelector('.selected').dataset.unit; let m = 0; switch(u){ case 'seconds': m=v/60; break; case 'minutes': m=v; break; case 'hours': m=v*60; break; case 'days': m=v*1440; break; case 'weeks': m=v*10080; break; case 'months': m=v*43200; break; } if(m>0&&currentEditingTaskId){startTimer(currentEditingTaskId,m);closeModal(timerModal);currentEditingTaskId=null;} });
    timerMenuCancelBtn.addEventListener('click', () => { if (currentEditingTaskId) stopTimer(currentEditingTaskId); closeModal(timerMenuModal); });
    timerDurationSlider.addEventListener('input', () => timerDurationDisplay.textContent = timerDurationSlider.value);
    timerUnitSelector.addEventListener('click', (e) => { const t = e.target.closest('.timer-unit-btn'); if (t) { timerUnitSelector.querySelector('.selected').classList.remove('selected'); t.classList.add('selected'); playSound('toggle'); } });
    addGroupForm.addEventListener('submit', (e) => { e.preventDefault(); const n = newGroupInput.value.trim(); if (n) { addGroup(n); newGroupInput.value = ''; closeModal(addGroupModal); } });
    
    addTaskTriggerBtnDaily.addEventListener('click', () => { currentListToAdd = 'daily'; weeklyGoalContainer.style.display = 'block'; addTaskModalTitle.textContent = 'Add Daily Quest'; weeklyGoalSlider.value = 0; updateGoalDisplay(weeklyGoalSlider, weeklyGoalDisplay); openModal(addTaskModal); focusOnDesktop(newTaskInput); });
    addStandaloneTaskBtn.addEventListener('click', () => { currentListToAdd = 'standalone'; weeklyGoalContainer.style.display = 'none'; addTaskModalTitle.textContent = 'Add Main Quest'; openModal(addTaskModal); focusOnDesktop(newTaskInput); });
    addGroupBtn.addEventListener('click', () => { openModal(addGroupModal); focusOnDesktop(newGroupInput); });
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
    [addTaskModal, editTaskModal, addGroupModal, settingsModal, confirmModal, timerModal, accountModal, manageAccountModal, document.getElementById('username-modal'), document.getElementById('google-signin-loader-modal'), friendsModal, shareQuestModal].forEach(m => { 
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
    const applySettings = () => { document.documentElement.style.setProperty('--accent', settings.accentColor); document.querySelectorAll('.color-swatch').forEach(s => s.classList.toggle('selected', s.dataset.color === settings.accentColor)); if(typeof settings.volume==='undefined') settings.volume=0.3; volumeSlider.value = settings.volume; const d = window.matchMedia('(prefers-color-scheme: dark)').matches; document.documentElement.classList.toggle('dark-mode', settings.theme === 'dark' || (settings.theme === 'system' && d)); document.querySelectorAll('.theme-btn').forEach(b => b.classList.remove('selected')); const s = document.querySelector(`.theme-btn[data-theme="${settings.theme}"]`); if(s)s.classList.add('selected'); };
    themeOptionsButtons.addEventListener('click', (e) => { const t = e.target.closest('.theme-btn'); if (t) { settings.theme = t.dataset.theme; saveState(); applySettings(); playSound('toggle'); } });
    colorOptions.addEventListener('click', (e) => { if(e.target.classList.contains('color-swatch')) { settings.accentColor = e.target.dataset.color; saveState(); applySettings(); } });
    volumeSlider.addEventListener('input', () => { settings.volume = parseFloat(volumeSlider.value); saveState(); });
    volumeSlider.addEventListener('change', () => playSound('toggle'));
    
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
    
    resetProgressBtn.addEventListener('click', () => showConfirm('Reset all progress?', 'This cannot be undone.', () => { playerData = { level: 1, xp: 0 }; dailyTasks = []; standaloneMainQuests = []; generalTaskGroups = []; renderAllLists(); saveState(); playSound('delete'); }));
    exportDataBtn.addEventListener('click', () => { const d = localStorage.getItem('anonymousUserData'); const b = new Blob([d || '{}'], {type: "application/json"}), a = document.createElement("a"); a.href = URL.createObjectURL(b); a.download = `procrasti-nope_guest_backup.json`; a.click(); });
    importDataBtn.addEventListener('click', () => importFileInput.click());
    importFileInput.addEventListener('change', (e) => { const f = e.target.files[0]; if(!f) return; showConfirm("Import Guest Data?", "This will overwrite current guest data.", () => { const r = new FileReader(); r.onload = (e) => { localStorage.setItem('anonymousUserData', e.target.result); initialLoad(); }; r.readAsText(f); }); e.target.value = ''; });
    document.body.addEventListener('mouseover', e => { const t = e.target.closest('.btn, .color-swatch, .complete-btn, .main-title'); if (!t || (e.relatedTarget && t.contains(e.relatedTarget))) return; playSound('hover'); });
    
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
            manageAccountModal.querySelector('#update-email-form').style.display = 'block';
            manageAccountModal.querySelector('#update-password-form').style.display = 'block';
            manageAccountModal.querySelector('#update-username-form').style.display = 'block';
        }
        openModal(manageAccountModal);
    });

    const reauthForm = document.getElementById('reauth-form');
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
        } catch (error) {
            errorEl.textContent = getCoolErrorMessage(error);
        }
    });

    const updateEmailForm = document.getElementById('update-email-form');
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

    const updatePasswordForm = document.getElementById('update-password-form');
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

    const updateUsernameForm = document.getElementById('update-username-form');
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
            const currentUsername = userDocSnap.data().username;

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
    
    function initSortable() {
        function onTaskDrop(evt) {
            document.body.classList.remove('is-dragging');
            const taskId = evt.item.dataset.id;
            if (!taskId) return;
            const { task, list: sourceListArray } = findTaskAndContext(taskId);
            if (!task || !sourceListArray) return;
            const originalIndex = sourceListArray.findIndex(t => t.id === taskId);
            if (originalIndex > -1) sourceListArray.splice(originalIndex, 1);
             else return; 

            const toListEl = evt.to;
            const toListId = toListEl.id;
            const toGroupId = toListEl.dataset.groupId;
            let destListArray;

            if (toListId === 'daily-task-list') destListArray = dailyTasks;
            else if (toListId === 'standalone-task-list') destListArray = standaloneMainQuests;
            else if (toGroupId) {
                const group = generalTaskGroups.find(g => g.id === toGroupId);
                if (group) {
                    if (!group.tasks) group.tasks = [];
                    destListArray = group.tasks;
                }
            }

            if (!destListArray) {
                sourceListArray.splice(originalIndex, 0, task);
                return;
            }
            
            destListArray.splice(evt.newIndex, 0, task);
            saveState();
            renderAllLists();
        }

        const commonTaskOptions = {
            animation: 150,
            delay: 500, // Apply a delay to prevent accidental drags on quick clicks
            delayOnTouchOnly: false, // Ensure delay applies to both touch and mouse
            onStart: () => document.body.classList.add('is-dragging'),
            onEnd: onTaskDrop 
        };

        new Sortable(dailyTaskListContainer, { ...commonTaskOptions, group: 'dailyQuests' });
        new Sortable(standaloneTaskListContainer, { ...commonTaskOptions, group: 'mainQuests' });
        document.querySelectorAll('.task-list-group').forEach(listEl => {
            new Sortable(listEl, { ...commonTaskOptions, group: 'mainQuests' });
        });
        new Sortable(generalTaskListContainer, {
            animation: 150,
            handle: '.main-quest-group-header',
            delay: 500, // Apply delay for group dragging as well
            delayOnTouchOnly: false, // Ensure delay applies to both touch and mouse
            onStart: () => document.body.classList.add('is-dragging'),
            onEnd: (e) => {
                document.body.classList.remove('is-dragging');
                const [item] = generalTaskGroups.splice(e.oldIndex, 1);
                generalTaskGroups.splice(e.newIndex, 0, item);
                saveState();
            }
        });
    }

    function createConfetti(el) { if(!el) return; const r = el.getBoundingClientRect(); createFullScreenConfetti(false, { x: r.left + r.width / 2, y: r.top + r.height / 2 }); }
    function createFullScreenConfetti(party, o = null) {
        for (let i = 0; i < (party ? 200 : 100); i++) {
            const c = document.createElement('div'); c.className = 'confetti';
            const sx = o ? o.x : Math.random()*window.innerWidth, sy = o ? o.y : -20;
            c.style.left=`${sx}px`; c.style.top=`${sy}px`; c.style.backgroundColor = ['var(--accent-pink)','var(--accent-blue)','var(--accent-green)','var(--accent-orange)','var(--accent-purple)'][Math.floor(Math.random()*5)];
            document.body.appendChild(c);
            const a = Math.random()*Math.PI*2, v=50+Math.random()*100, ex=Math.cos(a)*v*(Math.random()*5), ey=(Math.sin(a)*v)+(window.innerHeight-sy);
            c.animate([{transform:'translate(0,0) rotate(0deg)',opacity:1},{transform:`translate(${ex}px, ${ey}px) rotate(${Math.random()*720}deg)`,opacity:0}],{duration:3000+Math.random()*2000,easing:'cubic-bezier(0.1,0.5,0.5,1)'}).onfinish=()=>c.remove();
        }
        if(party){const p=document.createElement('div');p.className='party-time-overlay';document.body.appendChild(p);setTimeout(()=>p.remove(),5000);}
    }
    const renderAllLists = () => { renderSharedQuests(); renderDailyTasks(); renderStandaloneTasks(); renderGeneralTasks(); checkOverdueTasks(); initSortable(); resumeTimers(); };
    
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
    
    function listenForFriendRequests() {
        if (!user) return;
        if (unsubscribeFromFriends) unsubscribeFromFriends();
        
        const userDocRef = doc(db, "users", user.uid);
        unsubscribeFromFriends = onSnapshot(userDocRef, (doc) => {
            if (doc.exists()) {
                const requests = doc.data().friendRequests || [];
                const requestCount = requests.length;
                const badges = [friendRequestCountBadge, friendRequestCountBadgeMobile, friendRequestCountBadgeModal];
                badges.forEach(badge => {
                     if (requestCount > 0) {
                        badge.textContent = requestCount;
                        badge.style.display = 'flex';
                    } else {
                        badge.style.display = 'none';
                    }
                });

                // FIX: Always re-render friends and requests when the snapshot updates
                // This ensures the list is fresh if the modal is open, or will be fresh
                // when the modal is next opened.
                renderFriendsAndRequests();
            }
        }, (error) => {
            console.error("Error listening to friend requests: ", getCoolErrorMessage(error));
        });
    }

    async function renderFriendsAndRequests() {
        if (!user) return;
        
        const userDocRef = doc(db, "users", user.uid);
        const userDoc = await getDoc(userDocRef);
        if (!userDoc.exists()) return;

        const userData = userDoc.data();
        const friendUIDs = userData.friends || [];
        const requestUIDs = userData.friendRequests || [];
        
        if (friendUIDs.length === 0) {
            friendsListContainer.innerHTML = `<p style="text-align: center; padding: 1rem;">Go add some friends!</p>`;
        } else {
            friendsListContainer.innerHTML = '';
            const friendsQuery = query(collection(db, "users"), where(documentId(), 'in', friendUIDs));
            const friendDocs = await getDocs(friendsQuery);
            friendDocs.forEach(doc => {
                 const friend = doc.data();
                 const level = friend.appData?.playerData?.level || 1;
                 const friendEl = document.createElement('div');
                 friendEl.className = 'friend-item';
                 friendEl.innerHTML = `<div class="friend-level-display">LVL ${level}</div><span class="friend-name">${friend.username}</span><div class="friend-item-actions"><button class="btn icon-btn remove-friend-btn" data-uid="${doc.id}" aria-label="Remove friend">&times;</button></div>`;
                 friendsListContainer.appendChild(friendEl);
            });
        }

        if (requestUIDs.length === 0) {
            friendRequestsContainer.innerHTML = `<p style="text-align: center; padding: 1rem;">No new requests.</p>`;
        } else {
            friendRequestsContainer.innerHTML = '';
            const requestsQuery = query(collection(db, "users"), where(documentId(), 'in', requestUIDs));
            const requestDocs = await getDocs(requestsQuery);
            requestDocs.forEach(doc => {
                const requestUser = doc.data(); // FIX: Changed requestDoc to doc
                const requestEl = document.createElement('div');
                requestEl.className = 'friend-request-item';
                requestEl.innerHTML = `<span>${requestUser.username}</span><div class="friend-request-actions"><button class="btn icon-btn accept-request-btn" data-uid="${doc.id}" aria-label="Accept request">&#10003;</button><button class="btn icon-btn decline-request-btn" data-uid="${doc.id}" aria-label="Decline request">&times;</button></div>`;
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
        const currentUsername = currentUserDoc.exists() ? currentUserDoc.data().username : null;

        // FIX: Prevent sending friend request to self
        if (currentUsername && usernameToFind === currentUsername) {
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
        const targetUserDocRef = doc(db, "users", targetUserId);
        
        try {
            await updateDoc(targetUserDocRef, {
                friendRequests: arrayUnion(user.uid)
            });
            friendStatusMessage.textContent = `Friend request sent to ${usernameToFind}!`;
            friendStatusMessage.style.color = 'var(--accent-green-light)';
            searchUsernameInput.value = '';
        } catch (error) {
            friendStatusMessage.textContent = "Could not send request.";
            friendStatusMessage.style.color = 'var(--accent-red-light)';
            console.error("Error sending friend request:", getCoolErrorMessage(error));
        }
    }
    
    async function handleRequestAction(e, action) {
        const button = e.target.closest('button');
        if (!button) return;

        const senderUid = button.dataset.uid;
        const currentUserRef = doc(db, "users", user.uid);
        
        const batch = writeBatch(db);
        batch.update(currentUserRef, { friendRequests: arrayRemove(senderUid) });

        if (action === 'accept') {
            const senderUserRef = doc(db, "users", senderUid);
            batch.update(currentUserRef, { friends: arrayUnion(senderUid) });
            batch.update(senderUserRef, { friends: arrayUnion(user.uid) });
        }
        
        await batch.commit();
        renderFriendsAndRequests();
    }
    
    async function removeFriend(e) {
        const button = e.target.closest('button');
        if (!button) return;
        const friendUidToRemove = button.dataset.uid;
        
        showConfirm("Remove Friend?", "Are you sure you want to remove this friend?", async () => {
            const currentUserRef = doc(db, "users", user.uid);
            const friendUserRef = doc(db, "users", friendUidToRemove);
            
            const batch = writeBatch(db);
            batch.update(currentUserRef, { friends: arrayRemove(friendUidToRemove) });
            batch.update(friendUserRef, { friends: arrayRemove(user.uid) });
            
            await batch.commit();
            renderFriendsAndRequests();
        });
    }

    friendsBtnDesktop.addEventListener('click', () => {
        openModal(friendsModal);
        renderFriendsAndRequests();
    });
    
    addFriendForm.addEventListener('submit', handleAddFriend);
    
    friendRequestsContainer.addEventListener('click', e => {
         if (e.target.closest('.accept-request-btn')) handleRequestAction(e, 'accept');
         if (e.target.closest('.decline-request-btn')) handleRequestAction(e, 'decline');
    });
    
    friendsListContainer.addEventListener('click', e => {
        if (e.target.closest('.remove-friend-btn')) removeFriend(e);
    });
    
    friendsModal.querySelector('.form-toggle').addEventListener('click', (e) => {
        if (e.target.matches('.toggle-btn')) {
            const tab = e.target.dataset.tab;
            friendsModal.querySelectorAll('.toggle-btn').forEach(btn => btn.classList.remove('active'));
            e.target.classList.add('active');
            friendsModal.querySelectorAll('.tab-content').forEach(form => form.classList.toggle('active', form.id === `${tab}-tab`));
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
            renderFriendsAndRequests();
        } else {
            document.querySelectorAll('.task-group').forEach(group => {
                group.classList.toggle('mobile-visible', group.dataset.section === section);
            });
        }
        playSound('toggle');
    });

    deleteAccountBtn.addEventListener('click', () => {
        showConfirm('Delete Account?', 'This action is irreversible and will permanently delete your account and all associated data.', async () => {
            try {
                const isGoogleUser = currentUser.providerData.some(p => p.providerId === 'google.com');
                
                if (!isGoogleUser) {
                   const password = document.getElementById('reauth-password').value;
                   const credential = EmailAuthProvider.credential(currentUser.email, password);
                   await reauthenticateWithCredential(currentUser, credential);
                }

                const userDocRef = doc(db, "users", currentUser.uid);
                const userDocSnap = await getDoc(userDocRef);
                const username = userDocSnap.exists() ? userDocSnap.data().username : null;

                const batch = writeBatch(db);
                batch.delete(userDocRef);
                if (username) {
                    const usernameDocRef = doc(db, "usernames", username);
                    batch.delete(usernameDocRef);
                }

                await deleteUser(currentUser);
                
                await batch.commit();

                closeModal(manageAccountModal);
                signOut(auth);
                window.location.reload(); 
            } catch (error) {
                console.error("Error deleting account:", error);
                const errorEl = document.getElementById('update-password-error');
                errorEl.textContent = getCoolErrorMessage(error);
            }
        });
    });

    async function loadUserSession() {
        await initialLoad();
        await updateUserUI();
        await promptForUsernameIfNeeded();
        await updateUserUI(); // Update UI again in case username was just set
        checkDailyReset();
        resumeTimers();
    }
    
    // --- SHARED QUESTS LOGIC ---
    
    function listenForSharedQuests() {
        if (!user) return;
        if (unsubscribeFromSharedQuests) unsubscribeFromSharedQuests();

        const q = query(collection(db, "sharedQuests"), where("participants", "array-contains", user.uid));
        
        unsubscribeFromSharedQuests = onSnapshot(q, (querySnapshot) => {
            const newSharedQuests = [];
            querySnapshot.forEach((doc) => {
                newSharedQuests.push({ ...doc.data(), id: doc.id, questId: doc.id });
            });
            
            // Check for updates
            newSharedQuests.forEach(newQuest => {
                const oldQuest = sharedQuests.find(q => q.id === newQuest.id);
                if (oldQuest) {
                    const friendJustCompleted = (newQuest.friendUid === user.uid && !oldQuest.ownerCompleted && newQuest.ownerCompleted) || (newQuest.ownerUid === user.uid && !oldQuest.friendCompleted && newQuest.friendCompleted);
                    if (friendJustCompleted) {
                        const taskEl = document.querySelector(`.task-item[data-id="${newQuest.id}"]`);
                        if(taskEl) {
                           taskEl.classList.add('friend-completed-pulse');
                           taskEl.addEventListener('animationend', () => taskEl.classList.remove('friend-completed-pulse'), {once: true});
                           playSound('friendComplete');
                        }
                    }
                }
            });

            sharedQuests = newSharedQuests;
            renderAllLists();
        }, (error) => {
            console.error("Error listening for shared quests:", getCoolErrorMessage(error));
        });
    }

    async function openShareModal(questId) {
        const { task } = findTaskAndContext(questId);
        if (!task || task.shared) {
            console.error("Task is not sharable or already shared.");
            return;
        }

        shareQuestIdInput.value = questId;
        shareQuestFriendList.innerHTML = '<div class="loader-box" style="margin: 2rem auto;"></div>';
        openModal(shareQuestModal);

        const userDoc = await getDoc(doc(db, "users", user.uid));
        const friendUIDs = userDoc.data().friends || [];
        
        if (friendUIDs.length === 0) {
            shareQuestFriendList.innerHTML = '<p style="text-align: center; padding: 1rem;">You need friends to share quests with!</p>';
            return;
        }

        shareQuestFriendList.innerHTML = '';
        const friendsQuery = query(collection(db, "users"), where(documentId(), 'in', friendUIDs));
        const friendDocs = await getDocs(friendsQuery);

        friendDocs.forEach(friendDoc => {
            const friendData = friendDoc.data();
            const friendEl = document.createElement('div');
            friendEl.className = 'share-friend-item';
            friendEl.innerHTML = `
                <div class="friend-level-display">LVL ${friendData.appData?.playerData?.level || 1}</div>
                <span class="friend-name">${friendData.username}</span>
                <button class="btn share-btn-action" data-uid="${friendDoc.id}" data-username="${friendData.username}">Share</button>
            `;
            shareQuestFriendList.appendChild(friendEl);
        });
    }

    shareQuestFriendList.addEventListener('click', async (e) => {
        const button = e.target.closest('.share-btn-action');
        if (button) {
            button.disabled = true;
            button.textContent = 'Sharing...';
            const questId = shareQuestIdInput.value;
            const friendUid = button.dataset.uid;
            const friendUsername = button.dataset.username;

            try {
                await shareQuest(questId, friendUid, friendUsername);
            } catch (error) {
                console.error("Failed to share quest:", error);
                // Optionally, add a user-facing error message here
                button.disabled = false;
                button.textContent = 'Share';
            } finally {
                closeModal(shareQuestModal);
            }
        }
    });

    async function shareQuest(questId, friendUid, friendUsername) {
        // --- FIX: Refactored shared quest logic ---
        const { task, list } = findTaskAndContext(questId);
        if (!task || !list) return;

        const userDoc = await getDoc(doc(db, "users", user.uid));
        const ownerUsername = userDoc.data().username;
        
        // 1. Let Firestore generate a new, unique ID for the shared quest
        const sharedQuestRef = doc(collection(db, "sharedQuests"));

        // 2. Create the shared quest document in the new collection
        const sharedQuestData = {
            text: task.text,
            ownerUid: user.uid,
            ownerUsername: ownerUsername,
            friendUid: friendUid,
            friendUsername: friendUsername,
            ownerCompleted: false,
            friendCompleted: false,
            createdAt: Date.now(),
            participants: [user.uid, friendUid]
        };
        await setDoc(sharedQuestRef, sharedQuestData);

        // 3. Remove the original local task, as it's now a cloud-synced quest
        const taskIndex = list.findIndex(t => t.id === questId);
        if (taskIndex > -1) {
            list.splice(taskIndex, 1);
        }

        // 4. Save the owner's updated task list. The `onSnapshot` listener
        // will add the new shared quest to the UI for both players.
        playSound('share');
        saveState();
        renderAllLists(); // Re-render immediately for the owner
    }
    
    async function completeSharedQuestPart(task, uncompleting = false) {
        const questId = task.questId;
        const sharedQuestRef = doc(db, "sharedQuests", questId);
        const isOwner = user && task.ownerUid === user.uid;
        
        const updateData = {};
        if (isOwner) {
            updateData.ownerCompleted = !uncompleting;
        } else {
            updateData.friendCompleted = !uncompleting;
        }
        
        await updateDoc(sharedQuestRef, updateData);
        
        if (!uncompleting) {
            playSound('complete');
            addXp(XP_PER_SHARED_QUEST / 2); // Award half XP for completing your part
        } else {
            playSound('delete');
             addXp(-(XP_PER_SHARED_QUEST / 2));
        }

        const updatedDoc = await getDoc(sharedQuestRef);
        if(updatedDoc.exists() && updatedDoc.data().ownerCompleted && updatedDoc.data().friendCompleted) {
            finishSharedQuest({ ...updatedDoc.data(), id: questId });
        } else {
            // The onSnapshot listener will handle the re-render
        }
    }
    
    async function finishSharedQuest(questData) {
        // --- FIX: Simplified to just delete the doc, letting the listener handle UI ---
        playSound('sharedQuestFinish');
        
        const taskEl = document.querySelector(`.task-item[data-id="${questData.id}"]`);
        if (taskEl) {
            taskEl.classList.add('shared-quest-finished');
            createConfetti(taskEl);
            // After the animation, delete the document. The snapshot listener will then remove the element.
            taskEl.addEventListener('animationend', async () => {
                await deleteDoc(doc(db, "sharedQuests", questData.id));
            }, { once: true });
        } else {
            // If the element isn't visible for some reason, just delete the doc immediately.
            await deleteDoc(doc(db, "sharedQuests", questData.id));
        }
    }


    const initOnce = () => {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', applySettings);
        showRandomQuote();
        setInterval(checkOverdueTasks, 60 * 1000);
    };

    const initAudioContext = () => {
        if (audioCtx && audioCtx.state === 'suspended') {
            audioCtx.resume().catch(e => console.error("AudioContext resume failed:", e));
        }
    };
    document.body.addEventListener('click', initAudioContext, { once: true });
    document.body.addEventListener('keydown', initAudioContext, { once: true });

    initOnce();
    await loadUserSession();

    return {
        isPartial: false,
        shutdown: () => {
             Object.keys(activeTimers).forEach(id => clearInterval(activeTimers[id]));
             if (unsubscribeFromFriends) unsubscribeFromFriends();
             if (unsubscribeFromSharedQuests) unsubscribeFromSharedQuests();
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
            // Check if username is already taken (read is allowed by rules)
            const usernamesRef = doc(db, "usernames", username);
            const usernameSnap = await getDoc(usernamesRef);
            if (usernameSnap.exists()) {
                throw new Error('This username is already taken.');
            }

            // Create the Firebase Auth user. This will trigger onAuthStateChanged.
            await createUserWithEmailAndPassword(auth, email, password);

            // The actual Firestore document creation (for 'users' and 'usernames')
            // will now be handled by the onAuthStateChanged listener and
            // promptForUsernameIfNeeded, ensuring request.auth is available.
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