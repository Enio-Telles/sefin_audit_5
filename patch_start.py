with open("start.js", "r", encoding="utf-8") as f:
    content = f.read()

python_spawn = """
  pythonServer.stderr.on("data", data => process.stderr.write(`\\x1b[31m[Python API ERR]\\x1b[0m ${data}`));
"""
python_spawn_new = """
  pythonServer.stderr.on("data", data => process.stderr.write(`\\x1b[31m[Python API ERR]\\x1b[0m ${data}`));
  pythonServer.on("error", (e) => err(`Python Spawn Error: ${e.message}`));
"""
content = content.replace(python_spawn.strip(), python_spawn_new.strip())


node_spawn = """
  nodeServer.stderr.on("data", data => process.stderr.write(`\\x1b[31m[Node App ERR]\\x1b[0m ${data}`));
"""
node_spawn_new = """
  nodeServer.stderr.on("data", data => process.stderr.write(`\\x1b[31m[Node App ERR]\\x1b[0m ${data}`));
  nodeServer.on("error", (e) => err(`Node Spawn Error: ${e.message}`));
"""
content = content.replace(node_spawn.strip(), node_spawn_new.strip())


global_err = """
main();
"""
global_err_new = """
process.on('uncaughtException', (e) => err(`Global Err: ${e.message}`));
main();
"""
content = content.replace(global_err.strip(), global_err_new.strip())

with open("start.js", "w", encoding="utf-8") as f:
    f.write(content)
