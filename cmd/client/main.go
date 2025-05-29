package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/chzyer/readline"
)

type Command struct {
	Name        string
	Description string
	Usage       string
	Example     string
}

type Client struct {
	conn net.Conn
}

func (c *Client) SendCommand(command string) (string, error) {
	err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to set write deadline: %w", err)
	}

	_, err = c.conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	err = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to set read deadline: %w", err)
	}

	buffer := make([]byte, 4096)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	return string(buffer[:n]), nil
}

func (c *Client) SendGetCommand(key string) (string, error) {
	command := fmt.Sprintf("GET %s\n", key)
	return c.SendCommand(command)
}

func (c *Client) SendSetCommand(key, value string) (string, error) {
	command := fmt.Sprintf("SET %s %s\n", key, value)
	return c.SendCommand(command)
}

func (c *Client) SendDelCommand(key string) (string, error) {
	command := fmt.Sprintf("DEL %s\n", key)
	return c.SendCommand(command)
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

type Config struct {
	Host    string
	Port    string
	Timeout time.Duration
}

var commands = map[string]Command{
	"GET": {
		Name:        "GET",
		Description: "Retrieve value by key",
		Usage:       "GET <key>",
		Example:     "GET mykey",
	},
	"SET": {
		Name:        "SET",
		Description: "Set key-value pair",
		Usage:       "SET <key> <value>",
		Example:     "SET mykey myvalue",
	},
	"DEL": {
		Name:        "DEL",
		Description: "Delete key-value pair",
		Usage:       "DEL <key>",
		Example:     "DEL mykey",
	},
	"HELP": {
		Name:        "HELP",
		Description: "Show available commands",
		Usage:       "HELP [command]",
		Example:     "HELP GET",
	},
	"EXIT": {
		Name:        "EXIT",
		Description: "Exit the client",
		Usage:       "EXIT",
		Example:     "EXIT",
	},
	"QUIT": {
		Name:        "QUIT",
		Description: "Exit the client",
		Usage:       "QUIT",
		Example:     "QUIT",
	},
}

func main() {
	fmt.Println("KV-DB Client")
	fmt.Println("Type 'HELP' for available commands or 'EXIT'/'QUIT' to quit")
	fmt.Println()

	client, err := getClient()
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		return
	}
	// Setup readline with autocomplete
	completer := readline.NewPrefixCompleter(createCompleterItems()...)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "kv-db> ",
		HistoryFile:     "/tmp/kv-db-history.tmp",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		fallbackClient(client)
		return
	}
	defer rl.Close() //nolint:errcheck

	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if shouldExit := processCommand(line, client); shouldExit {
			break
		}
	}
}

func getConfig() *Config {
	host := flag.String("host", "127.0.0.1", "Database server host")
	port := flag.String("port", "3223", "Database server port")
	timeout := flag.Duration("timeout", 10*time.Second, "Connection timeout")
	flag.Parse()
	cfg := &Config{
		Host:    *host,
		Port:    *port,
		Timeout: *timeout,
	}
	return cfg
}

func getClient() (*Client, error) {
	cfg := getConfig()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(cfg.Host, cfg.Port), cfg.Timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func createCompleterItems() []readline.PrefixCompleterInterface {
	var items []readline.PrefixCompleterInterface

	// Sort commands for consistent ordering
	var cmdNames []string
	for name := range commands {
		cmdNames = append(cmdNames, name)
	}
	sort.Strings(cmdNames)

	for _, name := range cmdNames {
		items = append(items, readline.PcItem(name))
	}

	return items
}

func processCommand(input string, client *Client) bool {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return false
	}

	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	switch cmd {
	case "HELP":
		handleHelp(args)
	case "EXIT", "QUIT":
		fmt.Println("Goodbye!")
		return true
	case "GET":
		handleGet(args, client)
	case "SET":
		handleSet(args, client)
	case "DEL":
		handleDel(args, client)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		fmt.Println("Type 'HELP' for available commands")
	}

	return false
}

func handleHelp(args []string) {
	if len(args) == 0 {
		fmt.Println("Available commands:")
		fmt.Println()

		// Sort commands for consistent display
		var cmdNames []string
		for name := range commands {
			cmdNames = append(cmdNames, name)
		}
		sort.Strings(cmdNames)

		for _, name := range cmdNames {
			cmd := commands[name]
			fmt.Printf("  %-8s %s\n", cmd.Name, cmd.Description)
		}

		fmt.Println()
		fmt.Println("Use 'HELP <command>' for detailed information about a specific command")
		return
	}

	// Show specific command help
	cmdName := strings.ToUpper(args[0])
	if cmd, exists := commands[cmdName]; exists {
		fmt.Printf("Command: %s\n", cmd.Name)
		fmt.Printf("Description: %s\n", cmd.Description)
		fmt.Printf("Usage: %s\n", cmd.Usage)
		fmt.Printf("Example: %s\n", cmd.Example)
	} else {
		fmt.Printf("Unknown command: %s\n", cmdName)
		fmt.Println("Type 'HELP' to see all available commands")
	}
}

func handleGet(args []string, client *Client) {
	if len(args) != 1 {
		fmt.Println("Error: GET requires exactly one argument")
		fmt.Println("Usage: GET <key>")
		return
	}

	key := args[0]
	result, err := client.SendGetCommand(key)
	if err != nil {
		fmt.Printf("Error sending GET command: %v\n", err)
		return
	}
	fmt.Printf("Value: %s\n", result)
}

func handleSet(args []string, client *Client) {
	if len(args) != 2 {
		fmt.Println("Error: SET requires exactly two arguments")
		fmt.Println("Usage: SET <key> <value>")
		return
	}

	key, value := args[0], args[1]
	result, err := client.SendSetCommand(key, value)
	if err != nil {
		fmt.Printf("Error sending SET command: %v\n", err)
		return
	}
	fmt.Printf("%s\n", result)
}

func handleDel(args []string, client *Client) {
	if len(args) != 1 {
		fmt.Println("Error: DEL requires exactly one argument")
		fmt.Println("Usage: DEL <key>")
		return
	}

	key := args[0]
	result, err := client.SendDelCommand(key)
	if err != nil {
		fmt.Printf("Error sending DEL command: %v\n", err)
		return
	}
	fmt.Printf("%s\n", result)
}

// Fallback client without readline (in case readline fails to initialize)
func fallbackClient(client *Client) {
	fmt.Println("Using fallback mode (no autocomplete)")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("kv-db> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if shouldExit := processCommand(line, client); shouldExit {
			break
		}
	}
}
