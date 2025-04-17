module.exports = async ({ github, context, core, diffJson }) => {
    try {
        // Check if the structure matches {"added": {}, "deleted": {}, "modified": {}}
        // Do not create comment if there are no configuration changes
        const isEmptyDiff = Object.keys(diffJson.added).length === 0 &&
          Object.keys(diffJson.deleted).length === 0 &&
          Object.keys(diffJson.modified).length === 0;
    
        if (isEmptyDiff) {
          console.log("No changes detected. Skipping comment creation.");
          return;
        }
    
        const diffOutput = JSON.stringify(diffJson, null, 2);
        const commentBody = `
### Config Diff Tool Output

\`\`\`json

${diffOutput}

\`\`\`
  
   
The above configuration changes are found in the PR. Please update the relevant release documentation if necessary.
    `;
    
        const { owner, repo } = context.repo;
        const issueNumber = context.payload.pull_request.number;
    
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
          console.log("A config diff comment already exists, deleting it...");
          // Update the existing comment
          await github.rest.issues.deleteComment({
            comment_id: existingComment.id,
            owner,
            repo,
          });
        }
    
        console.log("Creating a new config diff comment...");
        // Create a new comment
        await github.rest.issues.createComment({
          issue_number: issueNumber,
          owner,
          repo,
          body: commentBody,
        });
    
        // Set the status as FAILED if any configuration changes are detected
        console.log("Configuration changes detected: ",  diffOutput);
        core.setFailed("Configuration Changes Detected, Update release documents - if necessary");
      } catch (error) {
        core.setFailed(error.message);
      }
}