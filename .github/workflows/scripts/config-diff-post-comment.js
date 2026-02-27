module.exports = async ({ github, context, core, configDiff }) => {
  try {
    const { owner, repo } = context.repo;
    const issueNumber = context.payload.pull_request.number;

    // List all the comments
    const comments = await github.paginate(
      github.rest.issues.listComments, {
        owner,
        repo,
        issue_number: issueNumber,
        per_page: 100,
      }
    );

    // Check if any comment contains `/config check ok`
    const configCheckOkComment = comments.find(
      comment => comment.body.includes("/config check ok") && !comment.body.includes("### Config Diff Tool Output")
    );
    if (configCheckOkComment) {
      core.info("Found '/config check ok' comment. Returning with success.");
      return;
    }

    const existingComment = comments.find(comment => comment.body.includes("### Config Diff Tool Output"));

    // Do not create comment if there are no configuration changes
    if (!configDiff) {
      core.info("No changes detected. Skipping comment creation.");

      if (existingComment){
        // Remove any outdated configuration diff comments
        core.info("Existing config diff comment found. Deleting it...");
        await github.rest.issues.deleteComment({
            comment_id: existingComment.id,
            owner,
            repo,
        });
      }

      return;
    }

      const commentBody = `
### Config Diff Tool Output

\`\`\`diff

${configDiff}

\`\`\`

 
The above configuration changes are found in the PR. Please update the relevant release documentation if necessary.  
Ignore this comment if docs are already updated. To make the "Check ceph config changes" CI check pass, please comment \`/config check ok\` and re-run the test.  `;

      // List all files in the pull request
      core.info("Fetching list of files changed in the pull request...");
      const files = await github.paginate(
          github.rest.pulls.listFiles,
          {
            owner,
            repo,
            pull_number: issueNumber,
            per_page: 100,
          }
      );

      // Annotate YAML files
      files.forEach(file => {
        // Only annotate the `yaml.in` files present in `src/common/options` folder
        if (file.filename.endsWith(".yaml.in") && file.filename.startsWith("src/common/options/")) {
            core.info(`Annotating file: ${file.filename}`);
            // Show annotations only at the start of the file
            core.notice(
                `Configuration changes detected in ${file.filename}. Please update the release documentation if necessary. Ignore if already done`,
                {
                  title: "Configuration Change Detected",
                  file: file.filename,
                  startLine: 1,
                  endLine: 1,
                }
            );
        }
      });

      core.summary.addRaw(commentBody);
      await core.summary.write()

      if (existingComment) {
        // There might have been new configuration changes made after posting
        // the first comment. Hence replace the old comment with the new updated
        // changes
        core.info("A config diff comment already exists, updating it...");

        // Update the existing comment
        await github.rest.issues.updateComment ({
          comment_id: existingComment.id,
          owner,
          repo,
          body: commentBody,
        });
      } else {
        core.info("Creating a new config diff comment...");
        // Create a new comment
        await github.rest.issues.createComment({
          issue_number: issueNumber,
          owner,
          repo,
          body: commentBody,
        });

      }

      // Set the status as FAILED if any configuration changes are detected
      core.setFailed("Configuration Changes Detected, Update release documents - if necessary");
    } catch (error) {
      core.setFailed(error.message);
    }
}