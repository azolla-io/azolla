# Smart Commit Command

This command performs an intelligent git commit by analyzing changes and following best practices.

## Instructions

1. **Analyze current state**: Run `git status` and `git diff` to understand all changes
2. **Stage relevant files**: Add untracked and modified files that should be committed
3. **Generate commit message**: Create a commit message with:
   - A concise one-sentence title describing the main change
   - Only add bullet points complex changes that are not easily understandable by reading the commit
   - Follow conventional commit best practices (feat:, fix:, refactor:, etc.)
4. **Present for review**: Show the proposed commit message to the user and ask for confirmation
5. **Wait for approval**: Do NOT commit until the user explicitly approves the message
6. **Create commit**: Only after user confirmation, execute the commit with the approved message and push

## Commit Message Format

```
<type>: <concise one-sentence description>

• <important change 1>
• <important change 2>
• <important change 3>
```

## Requirements

- Never commit sensitive information (passwords, API keys, etc.)
- Ensure all staged changes are intentional and related
- Use clear, descriptive commit messages
- **Do NOT include any Claude Code promotional signatures**
- Focus on the most impactful changes in bullet points, ignore changes that are easy to understand or self-explanatory
- **ALWAYS ask for user confirmation before committing**
- Wait for explicit approval ("yes", "commit", "proceed", etc.) before executing commit
