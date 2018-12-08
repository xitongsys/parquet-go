package hdfs

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// ClientOptions represents the configurable options for a client.
type ClientOptions struct {
	Addresses []string
	Namenode  *rpc.NamenodeConnection
	User      string
}

// Username returns the value of HADOOP_USER_NAME in the environment, or
// the current system user if it is not set.
func Username() (string, error) {
	username := os.Getenv("HADOOP_USER_NAME")
	if username != "" {
		return username, nil
	}
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}

// NewClient returns a connected Client for the given options, or an error if
// the client could not be created.
func NewClient(options ClientOptions) (*Client, error) {
	var err error

	if options.User == "" {
		options.User, err = Username()
		if err != nil {
			return nil, err
		}
	}

	if options.Addresses == nil || len(options.Addresses) == 0 {
		options.Addresses, err = getNameNodeFromConf()
		if err != nil {
			return nil, err
		}
	}

	if options.Namenode == nil {
		options.Namenode, err = rpc.NewNamenodeConnectionWithOptions(
			rpc.NamenodeConnectionOptions{
				Addresses: options.Addresses,
				User:      options.User,
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &Client{namenode: options.Namenode}, nil
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under. If address is an empty string
// it will try and get the namenode address from the hadoop configuration
// files.
func New(address string) (*Client, error) {
	options := ClientOptions{}

	if address != "" {
		options.Addresses = []string{address}
	}

	return NewClient(options)
}

// getNameNodeFromConf returns namenodes from the system Hadoop configuration.
func getNameNodeFromConf() ([]string, error) {
	hadoopConf := LoadHadoopConf("")

	namenodes, nnErr := hadoopConf.Namenodes()
	if nnErr != nil {
		return nil, nnErr
	}
	return namenodes, nil
}

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
//
// Deprecated: Use NewClient with ClientOptions instead.
func NewForUser(address string, user string) (*Client, error) {
	return NewClient(ClientOptions{
		Addresses: []string{address},
		User:      user,
	})
}

// NewForConnection returns Client with the specified, underlying rpc.NamenodeConnection.
// You can use rpc.WrapNamenodeConnection to wrap your own net.Conn.
//
// Deprecated: Use NewClient with ClientOptions instead.
func NewForConnection(namenode *rpc.NamenodeConnection) *Client {
	client, _ := NewClient(ClientOptions{Namenode: namenode})
	return client
}

// ReadFile reads the file named by filename and returns the contents.
func (c *Client) ReadFile(filename string) ([]byte, error) {
	f, err := c.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	return ioutil.ReadAll(f)
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *Client) CopyToLocal(src string, dst string) error {
	remote, err := c.Open(src)
	if err != nil {
		return err
	}
	defer remote.Close()

	local, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer local.Close()

	_, err = io.Copy(local, remote)
	return err
}

// CopyToRemote copies the local file specified by src to the HDFS file at dst.
func (c *Client) CopyToRemote(src string, dst string) error {
	local, err := os.Open(src)
	if err != nil {
		return err
	}
	defer local.Close()

	remote, err := c.Create(dst)
	if err != nil {
		return err
	}
	defer remote.Close()

	_, err = io.Copy(remote, local)
	return err
}

func (c *Client) fetchDefaults() (*hdfs.FsServerDefaultsProto, error) {
	if c.defaults != nil {
		return c.defaults, nil
	}

	req := &hdfs.GetServerDefaultsRequestProto{}
	resp := &hdfs.GetServerDefaultsResponseProto{}

	err := c.namenode.Execute("getServerDefaults", req, resp)
	if err != nil {
		return nil, err
	}

	c.defaults = resp.GetServerDefaults()
	return c.defaults, nil
}

// Close terminates all underlying socket connections to remote server.
func (c *Client) Close() error {
	return c.namenode.Close()
}
