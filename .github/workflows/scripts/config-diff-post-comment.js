module.exports = async ({ github, context, core, configDiff }) => {
  try {
      // Do not create comment if there are no configuration changes
      if (!configDiff) {
        console.log("No changes detected. Skipping comment creation.");
        return;
      }

      const commentBody = `
### Config Diff Tool Output

\`\`\`diff

${configDiff}

\`\`\`

 
The above configuration changes are found in the PR. Please update the relevant release documentation if necessary.
  `;

      core.summary.addRaw(commentBody);
      await core.summary.write()

      const { owner, repo } = context.repo;
      const issueNumber = context.payload.pull_request.number;

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
            core.notice(
                `Configuration changes detected in ${file.filename}. Please update the relevant release documentation if necessary.`,
                {
                    title: "Configuration Change Detected",
                    file: file.filename,
                    startLine: 1,
                    endLine: 1,
                }
            );
        }
    });

      
      // List all the comments
      const comments = await github.paginate(
        github.rest.issues.listComments, {
          owner,
          repo,
          issue_number: issueNumber,
          per_page: 100,
        }
      );
  
      const existingComment = comments.find(comment => comment.body.includes("### Config Diff Tool Output"));
  
      if (existingComment) {
        core.info("A config diff comment already exists, deleting it...");
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