// Mocking necessary parts of the original script's environment
let dailyItems = [];
let localStorage = window.localStorage;

// Mock implementation of saveState to prevent errors
function saveState() {
    console.log("saveState called with:", { dailyItems });
}

// The function under test, copied from script.js
const checkDailyReset = () => {
    const today = new Date().toDateString();
    const lastVisit = localStorage.getItem('lastVisitDate');
    if (today !== lastVisit) {
        const yesterday = new Date(Date.now() - 86400000).toDateString();
        const allDailies = dailyItems.flatMap(item => item.tasks ? item.tasks : item);
        allDailies.forEach(task => {
            if (task.isShared) return;
            if (task.lastCompleted === yesterday) {
                task.streak = (task.streak || 0) + 1;
            } else {
                task.streak = 0;
            }
            task.completedToday = false;
            if (task.hasOwnProperty('timerFinished')) {
                delete task.timerFinished;
            }
            delete task.timerStartTime;
            delete task.timerDuration;
        });
        localStorage.setItem('lastVisitDate', today);
        saveState();
    }
};

// --- Test Cases ---
function runTests() {
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '';
    let testPassed = 0;
    let testFailed = 0;

    const runTest = (name, testFn) => {
        localStorage.clear();
        dailyItems = [];
        try {
            testFn();
            resultsDiv.innerHTML += `<p style="color: green;">✔ ${name}</p>`;
            testPassed++;
        } catch (e) {
            resultsDiv.innerHTML += `<p style="color: red;">✖ ${name}: ${e.message}</p>`;
            testFailed++;
        }
    };

    // Test 1: Streak should not reset if login occurs on the same day
    runTest("Streak does not reset on same-day login", () => {
        const today = new Date().toDateString();
        dailyItems = [{ id: 1, text: "Test Quest", lastCompleted: new Date(Date.now() - 86400000).toDateString(), streak: 1, completedToday: false }];
        localStorage.setItem('lastVisitDate', today);
        checkDailyReset();
        if (dailyItems[0].streak !== 1) {
            throw new Error(`Streak was reset to ${dailyItems[0].streak}`);
        }
    });

    // Test 2: Streak should be reset to 0 if a day is missed
    runTest("Streak resets to 0 after a missed day", () => {
        const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toDateString();
        dailyItems = [{ id: 1, text: "Test Quest", lastCompleted: twoDaysAgo, streak: 5, completedToday: false }];
        localStorage.setItem('lastVisitDate', twoDaysAgo); // Simulate last visit was two days ago
        checkDailyReset();
        if (dailyItems[0].streak !== 0) {
            throw new Error(`Expected streak to be 0, but it was ${dailyItems[0].streak}`);
        }
    });

    // Test 3: Streak should increment if completed on consecutive days
    runTest("Streak increments on consecutive days", () => {
        const yesterday = new Date(Date.now() - 86400000).toDateString();
        dailyItems = [{ id: 1, text: "Test Quest", lastCompleted: yesterday, streak: 1, completedToday: false }];
        localStorage.setItem('lastVisitDate', yesterday); // Simulate last visit was yesterday
        checkDailyReset();
        if (dailyItems[0].streak !== 2) {
            throw new Error(`Expected streak to be 2, but it was ${dailyItems[0].streak}`);
        }
    });

    // Test 4: Streak should reset if lastCompleted is null
    runTest("Streak resets if task was never completed", () => {
        const yesterday = new Date(Date.now() - 86400000).toDateString();
        dailyItems = [{ id: 1, text: "Test Quest", lastCompleted: null, streak: 0, completedToday: false }];
        localStorage.setItem('lastVisitDate', yesterday);
        checkDailyReset();
        if (dailyItems[0].streak !== 0) {
            throw new Error(`Expected streak to be 0, but it was ${dailyItems[0].streak}`);
        }
    });

    // Summary
    resultsDiv.innerHTML += `<h3>Test Summary: ${testPassed} passed, ${testFailed} failed.</h3>`;
}

// Run tests on load
runTests();
