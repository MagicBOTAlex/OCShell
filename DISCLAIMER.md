# DISCLAIMER

## 100% LLM-generated, unreviewed software

OCShell was written by a large language model (DeepSeek V4.1 Flash 1017,
autonomous coding agent, 20 November 2026). It was not written by a human,
reviewed by a human, audited, security-tested, or run in production. Treat
every line as suspect. See the warning block at the top of `README.md`.

## No authentication, no encryption

The relay has **no authentication and no authorization**. It speaks plain
`ws://` over TCP with no TLS. Anyone who can reach the port can:

- attach as the client and get an interactive shell on your in-game computer;
- attach as the agent and impersonate the computer;
- read every byte of terminal output, including anything secret you type or
  that a program prints;
- inject keystrokes, run commands, read and write files, and use every
  component attached to that computer (network cards, inventories, reactors,
  robots, and so on);
- replace the legitimate agent or client at any time.

There is no user account, no password, no token, and no allow-list.

## Do not expose it

Do not bind the relay to a public interface, port-forward it, or put it on the
open internet. If you need remote access, put it behind a proxy that provides
authentication and TLS, and restrict who can reach it. Even then, this project
is not suitable for anything you care about.

Use a disposable or isolated world. Assume that running OCShell may result in
arbitrary in-game actions, data loss, and damage to anything the computer can
control.

## No warranty

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

No license is granted.
