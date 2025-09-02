package cluster

import (
	"fmt"
	"os/exec"
	"strings"
)

func CloneRepoAndExecute(gitRepoLink, workingdir, command string) (string, error) {

	// clone the repo
	cloneCmd := exec.Command("git", "clone", gitRepoLink)
	cloneCmd.Dir = workingdir

	if err := cloneCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to clone repo: %v", err)
	}

	// execute the command on the repo
	parts := strings.Split(gitRepoLink, "/")
	repoName := strings.TrimSuffix(parts[len(parts)-1], ".git")
	cmd := exec.Command("bash", "-c", command)
	cmd.Dir = workingdir + "/" + repoName
	output, err := cmd.CombinedOutput()
	
	return string(output), err

}

