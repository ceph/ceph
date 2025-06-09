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

      const existingComment = comments.find(comment => comment.body.includes("### Config Diff Tool Output"));

      // Do not create comment if there are no configuration changes
      if (!configDiff) {
        core.info("No changes detected. Skipping comment creation.");

        if (existingComment){
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
  `;

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

        if (file.patch) {
            core.info(`Patch content for ${file.filename}:\n${file.patch}`);

            // A sample file.patch value looks like: "@@ -132,7 +132,7 @@ module Test @@ -1000,7 +1000,7 @@ module Test"
            // Extract all line numbers from the patch using regex
            const matches = [...file.patch.matchAll(/@@ -\d+,\d+ \+(\d+)(?:,(\d+))? @@/g)];
            matches.forEach(match => {
                const startLine = parseInt(match[1], 10); // Extracted start line
                const lineCount = match[2] ? parseInt(match[2], 10) : 1; // Number of lines changed
                const endLine = startLine + lineCount - 1; // Calculate the end line

                core.info(`startLine: ${startLine}, endLine: ${endLine}, linecount: ${lineCount}`)

                core.notice(
                    `Configuration changes detected. Please update the release documentation if necessary.`,
                    {
                        title: "Configuration Change Detected",
                        file: file.filename,
                        startLine: startLine,
                        endLine: endLine,
                    }
                );
            });
        } else {
            core.info(`No patch data available for file: ${file.filename}`);
        }
      });



      // Set action summary
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