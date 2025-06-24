# Smart Commit Command

This command performs an intelligent git commit by analyzing changes and following best practices.

## Instructions

1. **Analyze current state**: Run `git status` and `git diff` to understand all changes
2. **Stage relevant files**: Add untracked and modified files that should be committed
3. **Generate commit message**: Create a commit message with:
   - A concise one-sentence title describing the main change
   - Bullet points for all important changes (ignore minor ones)
   - Follow conventional commit best practices (feat:, fix:, refactor:, etc.)
4. **Create commit**: Execute the commit with the generated message
5. **Verify**: Run `git status` to confirm the commit succeeded

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
- Do NOT include any Claude Code promotional signatures
- Focus on the most impactful changes in bullet points

## Safety Checks

- Review all changes before committing
- Ensure no debugging code or temporary files are included
- Verify commit message accurately reflects the changes
