# GitHub Collaboration Guide - DataNexus BD

## Team Setup (5 Members)

This guide explains how to set up and collaborate on the DataNexus BD project using Git command line with 5 team members.

---

## Initial Setup

### Step 1: Create GitHub Repository (Project Leader Only)

1. Go to https://github.com and log in
2. Click **"New repository"**
3. Fill in details:
   - **Repository name**: `DataNexus_BD`
   - **Description**: Big Data Lakehouse Analytics Pipeline
   - **Visibility**: Private (or Public if required)
   - **Initialize**: Do NOT add README (we have one)
4. Click **"Create repository"**
5. Copy the repository URL: `https://github.com/YOUR_USERNAME/DataNexus_BD.git`

### Step 2: Add Team Members as Collaborators (Project Leader)

1. In GitHub repo → **Settings** → **Collaborators**
2. Click **"Add people"**
3. Add all 4 team member GitHub usernames
4. Send invitations

### Step 3: Each Team Member Accepts Invitation

1. Check email for GitHub invitation
2. Click **"Accept invitation"**
3. Confirm access to repository

---

## Git Configuration (All Members)

### Install Git
```bash
# macOS
brew install git

# Ubuntu/Debian
sudo apt-get install git

# Windows
# Download from: https://git-scm.com/download/win
```

### Configure Git Identity
```bash
# Set your name and email (use your GitHub email)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Verify configuration
git config --list
```

---

## Initial Project Setup (Project Leader)

### 1. Initialize Local Repository
```bash
# Navigate to your DataNexus_BD folder
cd /path/to/DataNexus_BD

# Initialize Git
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: DataNexus BD project setup"
```

### 2. Connect to GitHub Remote
```bash
# Add remote repository
git remote add origin https://github.com/YOUR_USERNAME/DataNexus_BD.git

# Verify remote
git remote -v

# Push to GitHub (creates main branch)
git push -u origin main
```

If you get an error about 'main' branch not existing:
```bash
# Create and switch to main branch
git branch -M main
git push -u origin main
```

---

## Cloning Repository (All Other Members)

### Each Team Member Runs:
```bash
# Navigate to where you want the project
cd ~/projects

# Clone the repository
git clone https://github.com/YOUR_USERNAME/DataNexus_BD.git

# Enter the directory
cd DataNexus_BD

# Verify files are present
ls -la
```

---

## Daily Git Workflow

### Workflow Overview
```
1. Pull latest changes
2. Create feature branch
3. Make changes
4. Commit changes
5. Push to GitHub
6. Create Pull Request (optional)
7. Merge to main
```

### 1. Pull Latest Changes (Before Starting Work)
```bash
# Always start by pulling latest changes
git checkout main
git pull origin main
```

### 2. Create Feature Branch
```bash
# Create branch for your work
# Naming convention: feature/description or fix/description
git checkout -b feature/add-esg-analysis

# Verify you're on the new branch
git branch
```

### 3. Make Changes
Edit files using your preferred editor or IDE.

### 4. Check Status & Stage Changes
```bash
# See what files changed
git status

# Add specific files
git add DataNexus_Pipeline.ipynb
git add data/stocks.csv

# Or add all changed files
git add .

# Check what will be committed
git status
```

### 5. Commit Changes
```bash
# Commit with descriptive message
git commit -m "Add ESG risk analysis to Silver layer"

# Or multi-line commit message
git commit -m "Add ESG analysis

- Implemented risk categorization
- Added sector aggregation
- Updated dashboard visualizations"
```

### 6. Push to GitHub
```bash
# Push your branch to GitHub
git push origin feature/add-esg-analysis

# If this is the first push for this branch
git push -u origin feature/add-esg-analysis
```

### 7. Merge to Main (Two Options)

**Option A: Direct Merge (Simple)**
```bash
# Switch to main branch
git checkout main

# Merge your feature branch
git merge feature/add-esg-analysis

# Push merged changes to GitHub
git push origin main

# Delete feature branch (optional)
git branch -d feature/add-esg-analysis
git push origin --delete feature/add-esg-analysis
```

**Option B: Pull Request (Recommended for Review)**
1. Go to GitHub repository page
2. Click **"Pull requests"** → **"New pull request"**
3. Set base: `main`, compare: `feature/add-esg-analysis`
4. Click **"Create pull request"**
5. Add description and request reviewers
6. After approval, click **"Merge pull request"**
7. Delete branch on GitHub

---

## Common Git Commands

### Viewing History
```bash
# View commit history
git log

# View compact history
git log --oneline

# View graphical history
git log --graph --oneline --all
```

### Checking Status
```bash
# See current status
git status

# See what changed in files
git diff

# See what's staged for commit
git diff --staged
```

### Undoing Changes
```bash
# Discard changes in a file (WARNING: loses changes)
git checkout -- filename.txt

# Unstage a file (keep changes)
git reset HEAD filename.txt

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Undo last commit (discard changes)
git reset --hard HEAD~1
```

### Branch Management
```bash
# List all branches
git branch

# List remote branches
git branch -r

# Switch to existing branch
git checkout branch-name

# Create and switch to new branch
git checkout -b new-branch-name

# Delete local branch
git branch -d branch-name

# Delete remote branch
git push origin --delete branch-name
```

---

## Handling Merge Conflicts

### When Conflicts Occur
```bash
# Attempt to merge
git merge feature/other-branch

# If conflict occurs, Git will show:
# Auto-merging DataNexus_Pipeline.ipynb
# CONFLICT (content): Merge conflict in DataNexus_Pipeline.ipynb
```

### Resolving Conflicts
1. Open conflicted file in editor
2. Look for conflict markers:
```
<<<<<<< HEAD
Your changes
=======
Their changes
>>>>>>> feature/other-branch
```
3. Manually edit to keep desired changes
4. Remove conflict markers
5. Save file
6. Stage resolved file: `git add DataNexus_Pipeline.ipynb`
7. Complete merge: `git commit -m "Resolve merge conflict in pipeline"`
8. Push: `git push origin main`

---

## Collaboration Scenarios

### Scenario 1: Two Members Working on Same File

**Member 1**:
```bash
git checkout -b feature/update-stocks
# Edit DataNexus_Pipeline.ipynb
git add DataNexus_Pipeline.ipynb
git commit -m "Update stocks data ingestion"
git push origin feature/update-stocks
git checkout main
git merge feature/update-stocks
git push origin main
```

**Member 2** (after Member 1 pushed):
```bash
git checkout main
git pull origin main  # Get Member 1's changes
git checkout -b feature/add-esg-dashboard
# Edit DataNexus_Pipeline.ipynb
git add DataNexus_Pipeline.ipynb
git commit -m "Add ESG dashboard"
git push origin feature/add-esg-dashboard
git checkout main
git merge feature/add-esg-dashboard  # May have conflict if same lines edited
# Resolve conflict if needed
git push origin main
```

### Scenario 2: Multiple Concurrent Features

**Workflow**:
1. Each member works on separate branch
2. Members push their branches independently
3. Project leader reviews and merges each branch
4. Team members pull latest main after merges

```bash
# Member 2: ESG Analysis
git checkout -b feature/esg-analysis
# ... work ...
git push origin feature/esg-analysis

# Member 3: Energy Dashboard
git checkout -b feature/energy-dashboard
# ... work ...
git push origin feature/energy-dashboard

# Project Leader: Merge both
git checkout main
git merge feature/esg-analysis
git push origin main
git merge feature/energy-dashboard
git push origin main

# All members: Pull merged changes
git checkout main
git pull origin main
```

---

## Best Practices

### 1. Commit Frequency
- Commit often (every logical change)
- One feature = one commit (or a few related commits)
- Don't commit broken code

### 2. Commit Messages
**Good**:
```
Add ESG risk categorization to Silver layer
Fix null handling in stocks data ingestion
Update README with setup instructions
```

**Bad**:
```
Update
Fixed stuff
More changes
```

### 3. Pull Before Push
```bash
# Always pull before pushing
git pull origin main
git push origin main
```

### 4. Branch Naming
- `feature/add-esg-analysis`
- `fix/null-handling-bug`
- `docs/update-readme`
- `refactor/optimize-silver-layer`

### 5. Don't Commit These Files
Add to `.gitignore`:
```
# Databricks artifacts
.ipynb_checkpoints/
*.log
__pycache__/

# Large files
*.parquet
*.delta

# Credentials
.env
secrets.json
```

---

## Emergency Commands

### "I messed up, start over"
```bash
# Discard all local changes
git reset --hard origin/main

# Get fresh copy from GitHub
git pull origin main
```

### "I committed to wrong branch"
```bash
# Copy commit hash
git log --oneline

# Switch to correct branch
git checkout correct-branch

# Apply commit
git cherry-pick <commit-hash>

# Switch back and undo
git checkout wrong-branch
git reset --hard HEAD~1
```

### "Someone force-pushed, my history is wrong"
```bash
# Backup your work
git branch backup-branch

# Reset to remote
git fetch origin
git reset --hard origin/main
```

---

## Quick Reference

### Daily Commands
```bash
# Start work
git checkout main
git pull origin main
git checkout -b feature/my-feature

# Save work
git add .
git commit -m "Description"
git push origin feature/my-feature

# Finish work
git checkout main
git merge feature/my-feature
git push origin main
```

### Team Sync
```bash
# Get everyone's changes
git checkout main
git pull origin main

# See who changed what
git log --oneline --graph --all
```

---

## GitHub Desktop (GUI Alternative)

If command line is challenging, use **GitHub Desktop**:
1. Download: https://desktop.github.com/
2. Clone repository via GUI
3. Commit/push/pull with buttons
4. Visual diff and merge conflict resolution

---

## Support Resources

- **Git Documentation**: https://git-scm.com/doc
- **GitHub Guides**: https://guides.github.com/
- **Git Cheat Sheet**: https://education.github.com/git-cheat-sheet-education.pdf
- **Interactive Git Tutorial**: https://learngitbranching.js.org/

---

## Contact

For Git/GitHub issues, contact the Project Leader or DevOps Engineer (Member 5).

---

**END OF GUIDE**
