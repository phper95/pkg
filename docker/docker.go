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

type Docker struct {
	ContainerID   string
	ContainerName string
}
type ContainerOptions struct {
	Name       string
	ImageName  string
	Options    map[string]string
	MountPath  string
	PortExpose string
}

func (d *Docker) IsInstalled() bool {
	return exec.Command("docker", "ps").Run() == nil
}

func (d *Docker) Start(c ContainerOptions) (string, error) {
	dockerArgs := d.getDockerRunOptions(c)
	command := exec.Command("docker", dockerArgs...)
	command.Stderr = os.Stderr
	result, err := command.Output()
	if err != nil {
		d.Stop()
		return string(result), err
	}
	return string(result), nil
}
func (d *Docker) Stop() error {
	fmt.Println("rm container", d.ContainerID, d.ContainerName)
	return exec.Command("docker", "rm", "-f", d.ContainerID).Run()
}

//docker [ "run", "-d", "--name" mysql-for-unittest -p 3306:3306
//-e MYSQL_USER=test -e MYSQL_PASSWORD=test -e MYSQL_DATABASE=shop
//-e MYSQL_ROOT_PASSWORD=root --tmpfs /var/lib/mysql mysql:5.7
func (d *Docker) getDockerRunOptions(c ContainerOptions) []string {
	portExpose := fmt.Sprintf("%s:%s", c.PortExpose, c.PortExpose)
	var args []string
	for key, val := range c.Options {
		args = append(args, []string{"-e", fmt.Sprintf("%s=%s", key, val)}...)
	}
	args = append(args, []string{"--tmpfs", c.MountPath}...)
	args = append(args, c.ImageName)
	dockerArgs := append([]string{"run", "-d", "--name", c.Name, "-p", portExpose}, args...)
	return dockerArgs
}

func (d *Docker) RemoveIfExists() error {
	command := exec.Command("docker", "ps", "-a", "-q", "-f", "name="+d.ContainerName)
	output, err := command.CombinedOutput()
	if err != nil {
		return err
	}
	d.ContainerID = strings.Trim(string(output), "\n")
	return d.Stop()
}
func (d *Docker) WaitForStartOrKill(timeout int) error {
	for t := 0; t < timeout; t++ {
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
	return errors.New("docker start timout so stopped")
}

func (d *Docker) getContainerStatus() string {
	command := exec.Command("docker", "ps", "-a", "--format", "{{.ID}}|{{.Status}}|{{.Ports}}|{{.Names}}")
	output, err := command.CombinedOutput()
	if err != nil {
		d.Stop()
		return DockerStatusExited
	}
	outputstr := string(output)
	outputstr = strings.TrimSpace(outputstr)
	dockerPsResponse := strings.Split(outputstr, "\n")
	for _, response := range dockerPsResponse {
		containerStatusData := strings.Split(response, "|")
		containerStatus := containerStatusData[1]
		containerName := containerStatusData[3]
		if containerName == d.ContainerName {
			if strings.HasPrefix(containerStatus, "Up ") {
				return DockerStatusRunning
			}
		}
		return DockerStatusStarting
	}
}
