from pathlib import Path
import re
import json
import os
from rich.console import Console
from rich.syntax import Syntax
from rich.text import Text
import sys

NOTEBOOKS_PATH = Path("./JupyterNotebooks")
CONFIGS_PATH = Path("./LOCALIZE_Configs")

def is_blank(line):
    return line.strip() == ""

def strip_comment(line):
    line = line.split("#", 1)[0]
    line = re.sub(r'("""|\'\'\')(.*?)\1', '', line)
    return line.rstrip()

def strip_structural(line):
    return "".join([token for token in line if token not in "{}[](),"])

def is_meaningful_line(line):
    if is_blank(line):
        return False
    if is_blank(strip_comment(line)):
        return False
    if is_blank(strip_structural(line)):
        return False
    return True

def strip_meaningless(lines):
    return [strip_comment(line) for line in lines if is_meaningful_line(line)]

def extract_code_lines_notebook(ipynb_path):
    """Extracts code lines from a Jupyter notebook (.ipynb) as a list of strings."""
    with open(ipynb_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)

    lines = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            lines.extend(cell.get("source", []))  # source is already a list of lines
            lines.append("\n")  # separate cells with a newline
    return lines

def keep_unique_lines(lines, other):
    out = []
    other = [line.strip() for line in other]
    for line in lines:
        stripped = line.strip()
        if stripped in other:
            other.remove(stripped)
        else:
            out.append(line)
    return out

def remove_unwanted_lines(lines, notebook):
    return [line for line in lines if notebook.stem not in line]

def color_diff_lines(lines, prefix, prefix_style):
    styled = []
    for line in lines:
        # Create a Text object
        text = Text(end = "")
        text.append(f"{prefix} ", style=prefix_style)
        code = line.strip("\n")
        syntax = Syntax(code, "python", theme="monokai", line_numbers=False, word_wrap=False) 
        hlcd = syntax.highlight(code)
        hlcd.rstrip()
        text.append(hlcd)  # only highlight the actual code
        styled.append(text)
    return styled

notebooks = sorted([Path(file) for file in os.listdir(NOTEBOOKS_PATH) if file.endswith(".ipynb")])

pr = strip_meaningless(extract_code_lines_notebook(NOTEBOOKS_PATH / notebooks[0]))
pr = remove_unwanted_lines(pr, notebooks[0])

chj = {
    "deleted": [],
    "added": []
}

print("Jupyter:")
for notebook in notebooks[1:5]:
    c = strip_meaningless(extract_code_lines_notebook(NOTEBOOKS_PATH / notebook))
    c = remove_unwanted_lines(c, notebook)

    deleted = keep_unique_lines(pr, c)
    added   = keep_unique_lines(c, pr)

    deleted_styled = color_diff_lines(deleted, "-", "bold red")
    added_styled   = color_diff_lines(added, "+", "bold green")

    pr = c

    console = Console(
        force_jupyter=False,
        force_terminal=True,
        file=sys.stdout,
    )

    console.print(f"  [bold red]lines deleted:[/] {len(deleted)}")
    console.print(f"  [green]lines added:[/] {len(added)}")
    print("____________________\n")
    chj["deleted"].append(len(deleted))
    chj["added"].append(len(added))

configs = sorted([Path(file) for file in os.listdir(CONFIGS_PATH)])[1:6]

def extract_code_lines_yaml(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.readlines()

def color_diff_lines(lines, prefix, prefix_style):
    styled = []
    for line in lines:
        # Create a Text object
        text = Text(end = "")
        text.append(f"{prefix} ", style=prefix_style)
        code = line.strip("\n")
        syntax = Syntax(code, "yaml", theme="monokai", line_numbers=False, word_wrap=False) 
        hlcd = syntax.highlight(code)
        hlcd.rstrip()
        text.append(hlcd)  # only highlight the actual code
        styled.append(text)
    return styled

print("Framework:")
chf = {
    "dvc.yaml":{
        "deleted": [],
        "added": []
    },
    "params.yaml":{
        "deleted": [],
        "added": []
    }
}
for cnf in ["dvc.yaml", "params.yaml"]:
    pr = strip_meaningless(extract_code_lines_yaml(CONFIGS_PATH / configs[0] / cnf))
    pr = remove_unwanted_lines(pr, configs[0])
    for config in configs[1:]:
        print(cnf)
        c = strip_meaningless(extract_code_lines_yaml(CONFIGS_PATH / config / cnf))
        c = remove_unwanted_lines(c, config)

        deleted = keep_unique_lines(pr, c)
        added   = keep_unique_lines(c, pr)

        deleted_styled = color_diff_lines(deleted, "-", "bold red")
        added_styled   = color_diff_lines(added, "+", "bold green")

        pr = c

        console = Console(
            force_jupyter=False,
            force_terminal=True,
            file=sys.stdout,
            width = 120
        )

        console.print(f"  [bold red]lines deleted:[/] {len(deleted)}")
        console.print(f"  [green]lines added:[/] {len(added)}")

        chf[cnf]["deleted"].append(len(deleted))
        chf[cnf]["added"].append(len(added))

    print("____________________\n")