import re

with open("requirements.txt", "r") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    line = line.strip()
    if line.startswith("polars"):
        new_lines.append("polars>=1.0.0,<2.0.0\n")
    elif line.startswith("oracledb"):
        new_lines.append("oracledb>=2.1.2,<3.0.0\n")
    elif line.startswith("fastapi"):
        new_lines.append("fastapi>=0.111.0,<1.0.0\n")
    elif line.startswith("uvicorn"):
        new_lines.append("uvicorn>=0.30.1,<1.0.0\n")
    elif line.startswith("pydantic"):
        new_lines.append("pydantic>=2.0.0,<3.0.0\n")
    elif line:
        new_lines.append(f"{line}\n")

if not any(l.startswith("pyarrow") for l in new_lines):
    new_lines.append("pyarrow>=14.0.0\n")

with open("requirements.txt", "w") as f:
    f.writelines(new_lines)
