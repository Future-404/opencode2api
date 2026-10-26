package gateway

import (
	_ "embed"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	wire "opencode2api/internal/protocol"
)

// The free tier accepts only agent-shaped requests, and its abuse checks expect
// the system prompt the official CLI sends. These assets are the CLI's own
// wording: the v1 text for /zen (OpenCode 1.x tool names) and the v2 text for
// /inference and /zen/go (OpenCode 2.x tool names). The v2 file is the v1 file
// with the tool names the 2.x CLI uses.
//
//go:embed opencode_prompt_v1.txt
var opencodePromptV1 string

//go:embed opencode_prompt_v2.txt
var opencodePromptV2 string

// opencodePromptMarker identifies a body that already carries the official CLI
// system prompt, so a client that sends it itself is left alone.
const opencodePromptMarker = "You are opencode, an interactive CLI tool"

// opencodeSkills is the built-in skill listing the CLI appends after its
// environment block. It is static, so it is part of the injected prompt.
const opencodeSkills = `Skills provide specialized instructions and workflows for specific tasks.
Use the skill tool to load a skill when a task matches its description.
<available_skills>
  <skill>
    <name>customize-opencode</name>
    <description>Use ONLY when the user is editing or creating opencode's own configuration: opencode.json, opencode.jsonc, files under .opencode/, or files under ~/.config/opencode/. Also use when creating or fixing opencode agents, subagents, skills, plugins, MCP servers, or permission rules. Do not use for the user's own application code, or for any project that is not configuring opencode itself.</description>
    <location>&lt;built-in&gt;</location>
  </skill>
</available_skills>`

var (
	opencodeEnvOnce sync.Once
	opencodeWorkDir string
)

// opencodeEnvironment renders the <env> block the CLI puts after its base
// prompt. The working directory is resolved once per process; a platform
// without one falls back to the official container path.
func opencodeEnvironment(model string) string {
	opencodeEnvOnce.Do(func() {
		directory := ""
		if current, err := os.Getwd(); err == nil {
			directory = filepath.Clean(strings.TrimSpace(current))
		}
		if directory == "" || directory == "." {
			directory = "/workspace"
		}
		opencodeWorkDir = directory
	})
	apiModel := strings.TrimSpace(strings.TrimPrefix(model, "opencode/"))
	if apiModel == "" {
		apiModel = "unknown"
	}
	platform := runtime.GOOS
	if platform == "windows" {
		platform = "win32"
	}
	return strings.Join([]string{
		"You are powered by the model named " + apiModel + ". The exact model ID is opencode/" + apiModel,
		"Here is some useful information about the environment you are running in:",
		"<env>",
		"  Working directory: " + opencodeWorkDir,
		"  Workspace root folder: " + opencodeWorkDir,
		"  Is directory a git repo: yes",
		"  Platform: " + platform,
		"  Today's date: " + time.Now().UTC().Format("Mon Jan 02 2006"),
		"</env>",
	}, "\n")
}

var officialModelNames = map[string]string{
	"beam-free":                       "Beam Free",
	"big-pickle":                      "Big Pickle",
	"cat-jam-free":                    "Cat Jam Free",
	"deepseek-v4-flash":               "DeepSeek V4 Flash",
	"deepseek-v4-flash-free":          "DeepSeek V4 Flash Free",
	"deepseek-v4-flash-vision-exp":    "DeepSeek V4 Flash Vision Exp",
	"deepseek-v4-pro":                 "DeepSeek V4 Pro",
	"deepseek-v4.1-flash":             "DeepSeek V4.1 Flash",
	"deepseek-v4.1-flash-free":        "DeepSeek V4.1 Flash Free",
	"exo-free":                        "Exo Free",
	"fledge-alpha-free":               "Fledge Alpha Free",
	"flint-40b-free":                  "Flint 40B Free",
	"glm-4.7-free":                    "GLM-4.7 Free",
	"glm-5-free":                      "GLM-5 Free",
	"glm-5.3-flash":                   "GLM-5.3-Flash",
	"glm-5.3-flash-free":              "GLM-5.3-Flash Free",
	"hy3-free":                        "Hy3 Free",
	"hy3-preview-free":                "Hy3 preview Free",
	"kimi-k2.5-free":                  "Kimi K2.5 Free",
	"laguna-s-2.1-free":               "Laguna S 2.1 Free",
	"ling-2.6-flash-free":             "Ling 2.6 Flash Free",
	"ling-3.0-flash-fin-free":         "Ling 3.0 Flash Fin Free",
	"ling-3.0-flash-free":             "Ling-3.0-flash Free",
	"ling-3.0-tiny-free":              "Ling-3.0-tiny Free",
	"ling-3.1-flash-free":             "Ling 3.1 Flash Free",
	"ll-alpha-free":                   "LL Alpha Free",
	"longcat-2.0-free":                "LongCat-2.0 Free",
	"longcat-2.5-preview-free":        "LongCat 2.5 Preview Free",
	"mimo-v2-flash-free":              "MiMo V2 Flash Free",
	"mimo-v2-omni-free":               "MiMo V2 Omni Free",
	"mimo-v2-pro-free":                "MiMo V2 Pro Free",
	"mimo-v2.5-free":                  "MiMo V2.5 Free",
	"mimo-v2.6-flash-free":            "MiMo-V2.6-Flash Free",
	"minimax-m2.1-free":               "MiniMax-M2.1 Free",
	"minimax-m2.5-free":               "MiniMax-M2.5 Free",
	"minimax-m3-free":                 "MiniMax-M3 Free",
	"minimax-m3.1-flash-free":         "MiniMax M3.1 Flash Free",
	"muse-spark-1.2-contributor-free": "Muse Spark 1.2 Free",
	"muse-spark-1.3-contributor-free": "Muse Spark 1.3 Free",
	"nemotron-3-super-free":           "Nemotron 3 Super Free",
	"nemotron-3-ultra-free":           "Nemotron 3 Ultra Free",
	"nemotron-3.5-lightning-free":     "Nemotron 3.5 Lightning Free",
	"nemotron-3.5-super-vl-free":      "Nemotron 3.5 Super VL Free",
	"north-mini-code-free":            "North Mini Code Free",
	"qwen3.6-plus-free":               "Qwen3.6 Plus Free",
	"ring-2.6-1t-free":                "Ring 2.6 1T Free",
	"space-bunny-free":                "Space Bunny Free",
	"step-5-preview-free":             "Step 5 Preview Free",
	"trinity-large-preview-free":      "Trinity Large Preview",
	"x-preview-f-free":                "Ox Alpha Free (Unlimited)",
}

// opencodeModelName returns the display name for a given model ID.
func opencodeModelName(apiModel string) string {
	if name, ok := officialModelNames[apiModel]; ok {
		return name
	}
	parts := strings.FieldsFunc(apiModel, func(r rune) bool {
		return r == '-' || r == '_'
	})
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

func opencodeSystemPromptV2(model string) string {
	apiModel := strings.TrimSpace(strings.TrimPrefix(model, "opencode/"))
	if apiModel == "" {
		apiModel = "deepseek-v4.1-flash-free"
	}
	modelName := opencodeModelName(apiModel)

	opencodeEnvOnce.Do(func() {
		directory := ""
		if current, err := os.Getwd(); err == nil {
			directory = filepath.Clean(strings.TrimSpace(current))
		}
		if directory == "" || directory == "." {
			directory = "/workspace"
		}
		opencodeWorkDir = directory
	})

	platform := runtime.GOOS
	if platform == "windows" {
		platform = "win32"
	}

	dateStr := time.Now().UTC().Format("Mon Jan 02 2006")

	prompt := opencodePromptV2
	prompt = strings.ReplaceAll(prompt, "{{MODEL_NAME}}", modelName)
	prompt = strings.ReplaceAll(prompt, "{{MODEL_ID}}", apiModel)
	prompt = strings.ReplaceAll(prompt, "{{DATE}}", dateStr)
	prompt = strings.ReplaceAll(prompt, "{{WORKDIR}}", opencodeWorkDir)
	prompt = strings.ReplaceAll(prompt, "{{PLATFORM}}", platform)
	return prompt
}

// opencodeSystemPrompt assembles the full CLI system prompt for model and the
// upstream generation's tool naming.
func opencodeSystemPrompt(model string, v1 bool) string {
	if v1 {
		return opencodePromptV1 + opencodeEnvironment(model) + "\n" + opencodeSkills
	}
	return opencodeSystemPromptV2(model)
}

// injectOfficialPrompt prepends the official CLI system prompt to a shaped
// free-tier payload. It reports whether the payload changed; a body that
// already carries the marker is left untouched, so repeated shaping cannot
// duplicate the prompt. System One payloads are not chat bodies and are never
// touched.
func injectOfficialPrompt(payload map[string]any, protocol wire.Protocol, v1 bool) bool {
	if payload == nil || protocol == wire.SystemOne {
		return false
	}
	model, _ := payload["model"].(string)
	prompt := opencodeSystemPrompt(model, v1)
	switch protocol {
	case wire.Chat:
		return injectChatPrompt(payload, prompt)
	case wire.Anthropic:
		return injectAnthropicPrompt(payload, prompt)
	case wire.Responses:
		return injectResponsesPrompt(payload, prompt)
	default:
		return false
	}
}

// injectChatPrompt folds every system/developer message into the single system
// message the 1.x and 2.x CLIs send, then prepends it. Client instructions keep
// their order and are separated from the official prompt by a blank line.
func injectChatPrompt(payload map[string]any, prompt string) bool {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	kept := make([]any, 0, len(messages)+1)
	var instructions []string
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			kept = append(kept, raw)
			continue
		}
		role, _ := message["role"].(string)
		if role != "system" && role != "developer" {
			kept = append(kept, raw)
			continue
		}
		content := message["content"]
		if carriesOpenCodePrompt(content) {
			return false
		}
		for _, part := range textFragments(content) {
			if part = strings.TrimSpace(part); part != "" {
				instructions = append(instructions, part)
			}
		}
	}
	system := prompt
	if joined := strings.Join(instructions, "\n"); joined != "" {
		system += "\n\n" + joined
	}
	payload["messages"] = append([]any{map[string]any{"role": "system", "content": system}}, kept...)
	return true
}

// injectAnthropicPrompt prepends the prompt to the top-level system value,
// keeping a caller's string or block list intact after it.
func injectAnthropicPrompt(payload map[string]any, prompt string) bool {
	system, exists := payload["system"]
	if exists && carriesOpenCodePrompt(system) {
		return false
	}
	if !exists {
		payload["system"] = prompt
		return true
	}
	switch value := system.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			payload["system"] = prompt
		} else {
			payload["system"] = prompt + "\n\n" + value
		}
	case []any:
		payload["system"] = append([]any{map[string]any{"type": "text", "text": prompt}}, value...)
	default:
		payload["system"] = prompt
	}
	return true
}

// injectResponsesPrompt prepends the prompt to the Responses instructions.
func injectResponsesPrompt(payload map[string]any, prompt string) bool {
	instructions, exists := payload["instructions"]
	if exists && carriesOpenCodePrompt(instructions) {
		return false
	}
	if text, ok := instructions.(string); exists && ok {
		if strings.TrimSpace(text) == "" {
			payload["instructions"] = prompt
		} else {
			payload["instructions"] = prompt + "\n\n" + text
		}
		return true
	}
	payload["instructions"] = prompt
	return true
}

// textFragments flattens a system or instruction value into its text: a plain
// string, a list of content blocks, or a block object carrying text or content.
func textFragments(value any) []string {
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{typed}
	case []any:
		var parts []string
		for _, item := range typed {
			parts = append(parts, textFragments(item)...)
		}
		return parts
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			return textFragments(text)
		}
		if content, ok := typed["content"]; ok {
			return textFragments(content)
		}
	}
	return nil
}

// carriesOpenCodePrompt reports whether any text inside value already contains
// the marker.
func carriesOpenCodePrompt(value any) bool {
	for _, part := range textFragments(value) {
		if strings.Contains(part, opencodePromptMarker) || strings.Contains(part, "You are an AI agent running in OpenCode") {
			return true
		}
	}
	return false
}
