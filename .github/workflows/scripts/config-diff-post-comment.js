module.exports = async ({ github, context, core, configDiff }) => {
  try {
      // Do not create comment if there are no configuration changes
      if (!configDiff) {
        core.info("No changes detected. Skipping comment creation.");
        return;
      }

      const commentBody = `
### Config Diff Tool Output

\`\`\`diff

${configDiff}

\`\`\`

 
The above configuration changes are found in the PR. Please update the relevant release documentation if necessary.
  `;

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

      // Print all the lines that were changed in the files
      core.info("Printing all changed lines in the pull request files...");
      files.forEach(file => {
        if (file.patch) {
          core.info(`Changes in file: ${file.filename}`);
          core.info(file.patch);
        }
      });

      // Filter for `.yaml.in` files in `src/common/options` directory
      const yamlInFiles = files.filter(file =>
        file.filename.startsWith("src/common/options/") && file.filename.endsWith(".yaml.in")
      );

      if (yamlInFiles.length === 0) {
        // The pull request branch might be behind the upstream `main` branch.
        // Some configuration changes in `configDiff` might not correspond to files in the PR.
        // If no `.yaml.in` files from `src/common/options` are part of the PR, skip further checks.
        core.info("No `.yaml.in` files found in `src/common/options`. Exiting...");
        return;
      }

      // Annotate YAML files
      yamlInFiles.forEach(file => {
        // Only annotate the `yaml.in` files present in `src/common/options` folder
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

      // Set action summary
      core.summary.addRaw(commentBody);
      await core.summary.write()
  
      const existingComment = comments.find(comment => comment.body.includes("### Config Diff Tool Output"));
  
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