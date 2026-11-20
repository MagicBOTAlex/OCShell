-- OCShell agent for OpenComputers 1 / OpenOS.
--
-- WARNING: LLM-generated. See DISCLAIMER.md.
--
-- Usage:
--   ocshell ws://host:port/agent          (raw TCP WebSocket)
--   ocshell https://host[/agent]          (HTTPS polling fallback)
--
-- Starts a nested OpenOS shell, streams the terminal output to the relay and
-- injects remote keystrokes into the machine. The ws:// mode needs an internet
-- card with TCP enabled; the https:// mode needs HTTP(S) only.

local component = require("component")
local computer = require("computer")
local shell = require("shell")
local term = require("term")
local tty = require("tty")

local AGENT = "OCShell/0.1"
local FRAME_INTERVAL = 0.05
local TERM_CHUNK = 4096
local CONNECT_TIMEOUT = 10

local bit = rawget(_G, "bit32")
if not bit then
  local ok, lib = pcall(require, "bit32")
  if ok then bit = lib end
end

local function log(fmt, ...)
  io.stderr:write(string.format("[ocshell] " .. fmt .. "\n", ...))
end

local function uptime()
  local ok, value = pcall(computer.uptime)
  return ok and tonumber(value) or 0
end

local function parse_url(url)
  local host, port, path = url:match("^ws://([^/:]+):(%d+)(/.*)$")
  if not host then
    host, path = url:match("^ws://([^/]+)(/.*)$")
    port = 80
  end
  if not host then
    return nil, "only ws://host:port/path URLs are supported"
  end
  return host, tonumber(port), path or "/"
end

local raw_args = table.pack(...)
local url = raw_args[1]
if type(url) ~= "string" then
  io.write("usage: ocshell ws://host:port/agent | https://host[/agent]\n")
  return 1
end

local http_mode = url:match("^https?://") ~= nil
local http_base
local host, port, path
local sock
local inbuf = ""

local inet = component.internet
if not inet then
  log("no internet card")
  return 1
end

if http_mode then
  http_base = url:gsub("/+$", ""):gsub("/agent$", "")
  if not http_base:match("^https?://[^/]+") then
    log("invalid HTTPS base URL: %s", url)
    return 1
  end
else
  host, port, path = parse_url(url)
  if not host then
    log("%s", tostring(port))
    return 1
  end

  local connect_ok, connect_err
  connect_ok, sock, connect_err = pcall(inet.connect, host, port)
  if not connect_ok then
    log("connect failed: %s", tostring(sock))
    return 1
  end
  if not sock then
    log("connect failed: %s", tostring(connect_err))
    return 1
  end

  local deadline = uptime() + CONNECT_TIMEOUT
  while true do
    local ok, connected, reason = pcall(sock.finishConnect)
    if not ok then
      log("connect failed: %s", tostring(connected))
      pcall(sock.close)
      return 1
    end
    if connected then break end
    if connected == nil and reason then
      log("connect failed: %s", tostring(reason))
      pcall(sock.close)
      return 1
    end
    if uptime() > deadline then
      log("connect timed out")
      pcall(sock.close)
      return 1
    end
    os.sleep(0.05)
  end

  local function write_all(data, timeout)
    local until_time = uptime() + (timeout or 5)
    while #data > 0 do
      local written, reason = sock.write(data)
      if type(written) ~= "number" then
        return nil, reason or "write failed"
      end
      if written > 0 then
        data = data:sub(written + 1)
      elseif uptime() > until_time then
        return nil, "write timed out"
      else
        os.sleep(0.01)
      end
    end
    return true
  end

  local key = "dGhlIHNhbXBsZSBub25jZQ=="
  local request = table.concat({
    "GET " .. path .. " HTTP/1.1",
    "Host: " .. host .. ":" .. tostring(port),
    "Upgrade: websocket",
    "Connection: Upgrade",
    "Sec-WebSocket-Key: " .. key,
    "Sec-WebSocket-Version: 13",
    "", "",
  }, "\r\n")

  local ok, err = write_all(request, 5)
  if not ok then
    log("handshake write failed: %s", tostring(err))
    pcall(sock.close)
    return 1
  end

  local header_deadline = uptime() + 5
  while not inbuf:find("\r\n\r\n", 1, true) do
    local data = sock.read(1024)
    if data == nil then
      log("connection closed during handshake")
      pcall(sock.close)
      return 1
    end
    if #data > 0 then inbuf = inbuf .. data end
    if uptime() > header_deadline then
      log("handshake timed out")
      pcall(sock.close)
      return 1
    end
    os.sleep(0.02)
  end

  local head_end = inbuf:find("\r\n\r\n", 1, true)
  local head = inbuf:sub(1, head_end - 1)
  inbuf = inbuf:sub(head_end + 4)
  if not head:match("^HTTP/%d+%.%d+ 101") then
    log("handshake rejected: %s", head:match("^[^\r\n]*") or "?")
    pcall(sock.close)
    return 1
  end
end

math.randomseed(os.time() + math.floor(uptime() * 1000))
if not http_mode then
  assert(bit, "no bit32 implementation available")
end

local outbuf = ""
local termbuf = ""
local disconnected = false
local real_pull_signal = computer.pullSignal
local keyboard_address = "no_system_keyboard"
do
  local ok, address = pcall(tty.keyboard)
  if ok and type(address) == "string" then
    keyboard_address = address
  end
end

local function encode_frame(opcode, payload)
  local length = #payload
  local header = { string.char(0x80 + opcode) }
  if length < 126 then
    header[#header + 1] = string.char(0x80 + length)
  elseif length < 65536 then
    header[#header + 1] = string.char(0x80 + 126)
    header[#header + 1] = string.char(math.floor(length / 256), length % 256)
  else
    header[#header + 1] = string.char(0x80 + 127)
    header[#header + 1] = string.char(0, 0, 0, 0)
    header[#header + 1] = string.char(
      math.floor(length / 0x1000000) % 0x100,
      math.floor(length / 0x10000) % 0x100,
      math.floor(length / 0x100) % 0x100,
      length % 0x100
    )
  end

  local mask = {
    math.floor(math.random(0, 255)),
    math.floor(math.random(0, 255)),
    math.floor(math.random(0, 255)),
    math.floor(math.random(0, 255)),
  }
  header[#header + 1] = string.char(mask[1], mask[2], mask[3], mask[4])

  local masked = {}
  for i = 1, length do
    masked[i] = string.char(
      bit.bxor(string.byte(payload, i), mask[(i - 1) % 4 + 1])
    )
  end
  return table.concat(header) .. table.concat(masked)
end

local function queue_frame(opcode, payload)
  outbuf = outbuf .. encode_frame(opcode, payload)
  if #outbuf > 1024 * 1024 then
    -- Keep the stream bounded. Dropping is better than freezing the computer.
    log("output backlog exceeded 1 MiB; dropping oldest data")
    outbuf = outbuf:sub(-512 * 1024)
  end
end

local last_output_progress = uptime()

local function flush_out()
  if #outbuf == 0 then
    last_output_progress = uptime()
    return
  end
  local written, reason = sock.write(outbuf)
  if type(written) ~= "number" then
    log("write failed: %s", tostring(reason))
    disconnected = true
    return
  end
  if written > 0 then
    outbuf = outbuf:sub(written + 1)
    last_output_progress = uptime()
  elseif uptime() - last_output_progress > 10 then
    log("output stalled for 10s; disconnecting")
    disconnected = true
  end
end

local function flush_term()
  if #termbuf > 0 then
    queue_frame(2, termbuf)
    termbuf = ""
  end
  flush_out()
end

local function queue_term(value)
  termbuf = termbuf .. tostring(value or "")
  if http_mode then
    if #termbuf > 1024 * 1024 then
      termbuf = termbuf:sub(-512 * 1024)
    end
  elseif #termbuf >= TERM_CHUNK then
    flush_term()
  end
end

local function process_input(text)
  for line in text:gmatch("[^\r\n]+") do
    local command, rest = line:match("^(%S+)%s*(.*)$")
    if command == "K" then
      local code, char, down = rest:match("^(%d+)%s+(%d+)%s+(%d+)$")
      if code then
        computer.pushSignal(
          down == "1" and "key_down" or "key_up",
          keyboard_address,
          tonumber(char),
          tonumber(code)
        )
      end
    elseif command == "T" then
      for pair in rest:gmatch("%x%x") do
        local byte = tonumber(pair, 16)
        computer.pushSignal("key_down", keyboard_address, byte, 0)
        computer.pushSignal("key_up", keyboard_address, byte, 0)
      end
    elseif command == "X" then
      computer.pushSignal("interrupted")
    elseif command == "S" then
      -- Client-side terminal size; the OC screen is fixed by the GPU.
    end
  end
end

local http_last = 0
local http_errors = 0
local http_inflight = false

local function http_agent_id()
  local ok, value = pcall(computer.address)
  if ok and type(value) == "string" and #value > 0 then
    return value
  end
  return "http-agent"
end

local function urlencode(value)
  return (tostring(value or ""):gsub("[^%w%-%._~]", function(character)
    return string.format("%%%02X", string.byte(character))
  end))
end

local function http_post(target, body)
  local internet = require("internet")
  local ok, handle, reason = pcall(internet.request, target, body, {
    ["content-type"] = "application/octet-stream",
    ["user-agent"] = AGENT,
  }, "POST")
  if not ok then return nil, tostring(handle) end
  if not handle then return nil, tostring(reason) end

  local chunks = {}
  local read_ok, err = pcall(function()
    for chunk in handle do
      chunks[#chunks + 1] = chunk
    end
  end)
  if not read_ok then return nil, tostring(err) end
  return table.concat(chunks)
end

local function http_tick(state)
  if http_inflight then return end
  local now = uptime()
  if state == "running" and now - http_last < 0.15 then return end
  http_last = now

  local body = termbuf
  termbuf = ""
  local query = table.concat({
    "agent=" .. urlencode(http_agent_id()),
    "state=" .. urlencode(state or ""),
    "version=" .. urlencode(AGENT),
    "uptime=" .. tostring(math.floor(now)),
  }, "&")

  http_inflight = true
  local response, err = http_post(http_base .. "/api/agent/poll?" .. query, body)
  http_inflight = false

  if response == nil then
    termbuf = body .. termbuf
    http_errors = http_errors + 1
    if http_errors == 1 or http_errors % 10 == 0 then
      log("HTTP poll failed (%d): %s", http_errors, tostring(err))
    end
    if http_errors >= 20 then
      log("too many HTTP failures; disconnecting")
      disconnected = true
    end
    return
  end

  http_errors = 0
  if #response > 0 then
    process_input(response)
  end
end

local function process_frames()
  while true do
    if #inbuf < 2 then return end
    local first, second = string.byte(inbuf, 1, 2)
    local opcode = first % 16
    local masked = second >= 128
    local length = second % 128
    local offset = 3

    if length == 126 then
      if #inbuf < 4 then return end
      length = string.byte(inbuf, 3) * 256 + string.byte(inbuf, 4)
      offset = 5
    elseif length == 127 then
      if #inbuf < 10 then return end
      length = 0
      for i = 3, 10 do length = length * 256 + string.byte(inbuf, i) end
      offset = 11
    end

    local mask
    if masked then
      if #inbuf < offset + 3 then return end
      mask = { string.byte(inbuf, offset, offset + 3) }
      offset = offset + 4
    end

    if #inbuf < offset + length - 1 then return end
    local payload = inbuf:sub(offset, offset + length - 1)
    inbuf = inbuf:sub(offset + length)

    if masked then
      local unmasked = {}
      for i = 1, #payload do
        unmasked[i] = string.char(
          bit.bxor(string.byte(payload, i), mask[(i - 1) % 4 + 1])
        )
      end
      payload = table.concat(unmasked)
    end

    if opcode == 1 then
      process_input(payload)
    elseif opcode == 8 then
      disconnected = true
    elseif opcode == 9 then
      queue_frame(10, payload)
    end
  end
end

local function poll_input()
  while true do
    local data, reason = sock.read(4096)
    if data == nil then
      if reason then log("read failed: %s", tostring(reason)) end
      disconnected = true
      return
    end
    if #data == 0 then break end
    inbuf = inbuf .. data
  end
  process_frames()
end

-- Capture all terminal text, including shell output and input echo.
local original_write = tty.stream.write
function tty.stream:write(value)
  queue_term(value)
  return original_write(self, value)
end

local old_clear = term.clear
if old_clear then
  term.clear = function(...)
    queue_term("\27[2J\27[H")
    return old_clear(...)
  end
end

if term.setCursor then
  local old_set_cursor = term.setCursor
  term.setCursor = function(x, y)
    if type(x) == "number" and type(y) == "number" then
      queue_term("\27[" .. math.floor(y) .. ";" .. math.floor(x) .. "H")
    end
    return old_set_cursor(x, y)
  end
end

local function check_connection()
  if disconnected then error("ocs:disconnect") end
end

local function wrapped_pull(timeout)
  local until_time = timeout and (uptime() + timeout) or nil
  while true do
    check_connection()
    if http_mode then
      http_tick("running")
    else
      flush_term()
      poll_input()
    end
    check_connection()

    local wait = FRAME_INTERVAL
    if until_time then
      local remaining = until_time - uptime()
      if remaining <= 0 then
        return
      end
      wait = math.min(wait, remaining)
    end

    local result = table.pack(real_pull_signal(wait))
    if result.n > 0 then
      if http_mode then http_tick("running") else flush_term() end
      return table.unpack(result, 1, result.n)
    end
    if until_time and uptime() >= until_time then
      if http_mode then http_tick("running") else flush_term() end
      return
    end
  end
end

computer.pullSignal = wrapped_pull
queue_term("\27[2J\27[H")
if http_mode then
  http_tick("starting")
else
  flush_term()

  local width, height = 80, 25
  if component.isAvailable and component.isAvailable("gpu") then
    local w, h = component.gpu.getResolution()
    if type(w) == "number" and type(h) == "number" then
      width, height = w, h
    end
  end
  queue_frame(1, "SIZE " .. width .. " " .. height .. "\n")
  flush_out()
end

local ok_shell, shell_err = pcall(function()
  local sh_file = shell.resolve("/bin/sh.lua")
  local sh_fn = assert(loadfile(sh_file))
  return sh_fn()
end)

computer.pullSignal = real_pull_signal
if http_mode then
  http_tick("stopped")
else
  flush_term()
  pcall(sock.close)
end

if not ok_shell and not tostring(shell_err):find("ocs:disconnect", 1, true) then
  log("shell ended with error: %s", tostring(shell_err))
end
return 0
