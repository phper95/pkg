package docker

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	DockerStatusExited   = "exited"
	DockerStatusRunning  = "running"
	DockerStatusStarting = "starting"
)

type DockerManager interface {
	Start(c ContainerOption) (string, error)
	Stop() error
}
type Docker struct {
	ContainerID   string
	ContainerName string
}

type ContainerOption struct {
	Name              string
	ContainerFileName string
	Options           map[string]string
	MountVolumePath   string
	PortExpose        string
}

func (d *Docker) IsInstalled() bool {
	command := exec.Command("docker", "ps")
	err := command.Run()
	if err != nil {
		return false
	}
	return true
}

func (d *Docker) Start(c ContainerOption) (string, error) {
	dockerArgs := d.getDockerRunOptions(c)
	fmt.Println(dockerArgs)
	return "", nil
	command := exec.Command("docker", dockerArgs...)
	command.Stderr = os.Stderr
	result, err := command.Output()
	if err != nil {
		return string(result), err
	}
	d.ContainerID = strings.TrimSpace(string(result))
	d.ContainerName = c.Name
	command = exec.Command("docker", "inspect", d.ContainerID)
	result, err = command.Output()
	if err != nil {
		d.Stop()
		return "", err
	}
	return string(result), nil
}

func (d *Docker) WaitForStartOrKill(timeout int) error {
	for tick := 0; tick < timeout; tick++ {
		containerStatus := d.getContainerStatus()
		if containerStatus == DockerStatusRunning {
			return nil
		}
		if containerStatus == DockerStatusExited {
			return nil
		}
		time.Sleep(time.Second)
	}
	d.Stop()
	return errors.New("Docker faile to start in given time period so stopped")
}

func (d *Docker) RemoveIfExists(c ContainerOption) error {
	command := exec.Command("docker", "ps", "-q", "-f", "name="+c.Name)
	output, err := command.CombinedOutput()
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return nil
	}
	return d.Stop()
}

func (d *Docker) getContainerStatus() string {
	command := exec.Command("docker", "ps", "-a", "--format", "{{.ID}}|{{.Status}}|{{.Ports}}|{{.Names}}")
	output, err := command.CombinedOutput()
	if err != nil {
		d.Stop()
		return DockerStatusExited
	}
	outputString := string(output)
	outputString = strings.TrimSpace(outputString)
	dockerPsResponse := strings.Split(outputString, "\n")
	for _, response := range dockerPsResponse {
		containerStatusData := strings.Split(response, "|")
		containerStatus := containerStatusData[1]
		containerName := containerStatusData[3]
		if containerName == d.ContainerName {
			if strings.HasPrefix(containerStatus, "Up ") {
				return DockerStatusRunning
			}
		}
	}
	return DockerStatusStarting
}

//return example: [run -d --name mysql-for-unittest -p 3306:3306 -e MYSQL_USER=test -e MYSQL_PASSWORD=test -e MYSQL_DATABASE=shop -e MYSQL_ROOT_PASSWORD=root --tmpfs /var/lib/mysql mysql:5.7]
func (d *Docker) getDockerRunOptions(c ContainerOption) []string {
	portExpose := fmt.Sprintf("%s:%s", c.PortExpose, c.PortExpose)
	var args []string
	for key, value := range c.Options {
		args = append(args, []string{"-e", fmt.Sprintf("%s=%s", key, value)}...)
	}
	args = append(args, []string{"--tmpfs", c.MountVolumePath, c.ContainerFileName}...)
	dockerArgs := append([]string{"run", "-d", "--name", c.Name, "-p", portExpose}, args...)
	return dockerArgs
}

func (d *Docker) Stop() error {
	return exec.Command("docker", "rm", "-f", d.ContainerID).Run()
}
