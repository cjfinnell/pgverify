package pgverify_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

var defaultTimeout = 10 * time.Second

// newDockerClient returns a docker client.
func newDockerClient() (*dockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("unable to create docker client: %w", err)
	}

	return &dockerClient{*cli}, nil
}

type dockerClient struct {
	client.Client
}

type containerConfig struct {
	image string
	ports []*portMapping
	env   []string
	cmd   []string
}

type portMapping struct {
	HostPort      string
	ContainerPort string
	Proto         string
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	asTCPAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unable assert net.Addr as net.TCPAddr")
	}

	return asTCPAddr.Port, nil
}

func (d dockerClient) runContainer(t *testing.T, ctx context.Context, config *containerConfig) (*container.ContainerCreateCreatedBody, error) {
	t.Helper()

	imageName, err := reference.ParseNormalizedNamed(config.image)
	if err != nil {
		return nil, fmt.Errorf("unable to normalize image name: %w", err)
	}

	fullName := imageName.String()

	container, err := d.createNewContainer(t, ctx, fullName, config.ports, config.env, config.cmd)
	if err != nil {
		return nil, fmt.Errorf("unable create container: %w", err)
	}

	if err = d.ContainerStart(ctx, container.ID, types.ContainerStartOptions{}); err != nil {
		return nil, fmt.Errorf("unable to start the container: %w", err)
	}

	t.Logf("container %s is started\n", container.ID)

	return container, nil
}

func (d dockerClient) createNewContainer(t *testing.T, ctx context.Context, image string, ports []*portMapping, env []string, cmd []string) (*container.ContainerCreateCreatedBody, error) {
	t.Helper()

	portBinding := nat.PortMap{}

	for _, portmap := range ports {
		hostBinding := nat.PortBinding{
			// TODO: Allow for host ips to be specified
			HostIP:   "0.0.0.0",
			HostPort: portmap.HostPort,
		}

		containerPort, err := nat.NewPort("tcp", portmap.ContainerPort)
		if err != nil {
			return nil, fmt.Errorf("unable to get the port: %w", err)
		}

		portBinding[containerPort] = []nat.PortBinding{hostBinding}
	}

	containerConfig := &container.Config{
		Image: image,
		Env:   env,
	}

	if len(cmd) > 0 {
		containerConfig.Cmd = cmd
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBinding,
	}
	networkingConfig := &network.NetworkingConfig{}

	var resp container.ContainerCreateCreatedBody

	var err error

	resp, err = d.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, "")
	if err != nil {
		if !client.IsErrNotFound(err) {
			return nil, fmt.Errorf("could not create container: %w", err)
		}

		out, err := d.ImagePull(ctx, image, types.ImagePullOptions{})
		if err != nil {
			return nil, fmt.Errorf("unable to pull image: %w", err)
		}
		defer out.Close()

		_, err = io.Copy(os.Stdout, out)
		if err != nil {
			return nil, err
		}

		resp, err = d.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, "")
		if err != nil {
			return nil, fmt.Errorf("could not create container: %w", err)
		}
	}

	return &resp, nil
}

func (d dockerClient) removeContainer(t *testing.T, ctx context.Context, id string) error {
	t.Helper()

	t.Logf("container %s is stopping\n", id)

	if err := d.ContainerStop(ctx, id, &defaultTimeout); err != nil {
		return fmt.Errorf("failed stopping container: %w", err)
	}

	t.Logf("container %s is stopped\n", id)

	err := d.Client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		// RemoveLinks=true causes "Error response from daemon: Conflict, cannot
		// remove the default name of the container"
		RemoveLinks: false,
		Force:       false,
	})
	if err != nil {
		return fmt.Errorf("failed removing container: %w", err)
	}

	t.Logf("container %s is removed\n", id)

	return nil
}
