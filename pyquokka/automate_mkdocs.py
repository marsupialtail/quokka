"""Automates Python scripts formatting, linting and Mkdocs documentation."""

import ast
import importlib
import re
import json, yaml
from collections import defaultdict
from pathlib import Path
from typing import Union, get_type_hints

def add_val(indices, value, data):
    if not len(indices):
        return
    element = data
    for index in indices[:-1]:
        element = element[index]
    element[indices[-1]] = value

def automate_mkdocs_from_docstring(
    mkdocs_dir: Union[str, Path], mkgendocs_f: str, repo_dir: Path, match_string: str
) -> dict:
    """Automates the -pages for mkgendocs package by adding all Python functions in a directory to the mkgendocs config.
    Args:
        mkdocs_dir (typing.Union[str, pathlib.Path]): textual directory for the hierarchical directory & navigation in Mkdocs
        mkgendocs_f (str): The configurations file for the mkgendocs package
        repo_dir (pathlib.Path): textual directory to search for Python functions in
        match_string (str): the text to be matches, after which the functions will be added in mkgendocs format
    Example:
        >>>
        >>> automate_mkdocs_from_docstring('scripts', repo_dir=Path.cwd(), match_string='pages:')
    Returns:
        list: list of created markdown files and their relative paths
    """
    p = repo_dir.glob('**/*.py')
    scripts = [x for x in p if x.is_file()]

    if Path.cwd() != repo_dir:  # look for mkgendocs.yml in the parent file if a subdirectory is used
        repo_dir = repo_dir.parent

    functions = defaultdict(dict)
    structure = fix(defaultdict)()
    full_repo_dir = str(repo_dir) + "/"
    for script in scripts:

        with open(script, 'r') as source:
            tree = ast.parse(source.read())
        funcs = {
        "classes":[],
        "functions":[]
        }
        for child in ast.iter_child_nodes(tree):
            try:
                if isinstance(child, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                    if child.name not in ['main']:

                        relative_path = str(script).replace(full_repo_dir, "").replace("/", ".").replace(".py", "")
                        module = importlib.import_module(relative_path)
                        f_ = getattr(module, child.name)
                        function = f_.__name__
                        if isinstance(child, (ast.ClassDef)):
                            funcs["classes"].append(function)
                        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            funcs["functions"].append(function)

            except Exception as e:
                print("trouble on importing " + script.stem)
                print("did not document " + child.name)
                print(str(e))
        if not funcs["classes"]:
            funcs.pop("classes")
        if not funcs["functions"]:
            funcs.pop("functions")
        if funcs:
            functions[script] = funcs
    with open(f'{repo_dir}/{mkgendocs_f}', 'r+') as mkgen_config:
        insert_string = ''
        for path, function_names in functions.items():
            relative_path = str(path).replace(full_repo_dir, "").replace(".py", "")
            insert_string += (
                f'  - page: "{mkdocs_dir}/{relative_path}.md"\n    '
                f'source: "{relative_path}.py"\n'    #functions:\n'
            )
            page = f"{mkdocs_dir}/{relative_path}"
            split_page = page.split("/")
            split_page = ["  - " + s for s in split_page]
            page += f".md"

            add_val(split_page, page, structure)
            for class_name, class_list in function_names.items():
                insert_string += f'    {class_name}:\n'
                f_string = ''
                for f in class_list:
                    insert_f_string = f'      - {f}\n'
                    f_string += insert_f_string

                insert_string += f_string
            insert_string += "\n"

        contents = mkgen_config.readlines()
        if match_string in contents[-1]:
            contents.append(insert_string)
        else:

            for index, line in enumerate(contents):
                if match_string in line and insert_string not in contents[index + 1]:

                    contents = contents[: index + 1]
                    contents.append(insert_string)
                    break

    with open(f'{repo_dir}/{mkgendocs_f}', 'w') as mkgen_config:
        mkgen_config.writelines(contents)

    return structure


def automate_nav_structure(
    mkdocs_dir: Union[str, Path], mkdocs_f: str, repo_dir: Path, match_string: str, structure: dict
) -> str:
    """Automates the -pages for mkgendocs package by adding all Python functions in a directory to the mkgendocs config.
    Args:
        mkdocs_dir (typing.Union[str, pathlib.Path]): textual directory for the hierarchical directory & navigation in Mkdocs
        mkgendocs_f (str): The configurations file for the mkgendocs package
        repo_dir (pathlib.Path): textual directory to search for Python functions in
        match_string (str): the text to be matches, after which the functions will be added in mkgendocs format
    Example:
        >>>
        >>> automate_mkdocs_from_docstring('scripts', repo_dir=Path.cwd(), match_string='pages:')
    Returns:
        str: feedback message
    """
    insert_string = yaml.safe_dump(json.loads(json.dumps(structure, indent=4))).replace("'", "")
    # print(structure)
    with open(f'{repo_dir}/{mkdocs_f}', 'r+') as mkgen_config:
        contents = mkgen_config.readlines()
        if match_string in contents[-1]:
            contents.append(insert_string)
        else:

            for index, line in enumerate(contents):
                if match_string in line and insert_string not in contents[index + 1]:

                    contents = contents[: index + 1]
                    contents.append(insert_string)
                    break

    with open(f'{repo_dir}/{mkdocs_f}', 'w') as mkgen_config:
        mkgen_config.writelines(contents)


def fix(f):
    """Allows creation of arbitrary length dict item
    Args:
        f (type): Description of parameter `f`.
    Returns:
        type: Description of returned object.
    """
    return lambda *args, **kwargs: f(fix(f), *args, **kwargs)



def indent(string: str) -> int:
    """Count the indentation in whitespace characters.
    Args:
        string (str): text with indents
    Returns:
        int: Number of whitespace indentations
    """
    return sum(4 if char == '\t' else 1 for char in string[: -len(string.lstrip())])

def main():
    """Execute when running this script."""
    python_tips_dir = Path.cwd().joinpath('')
    # python_tips_dir = Path.cwd().joinpath("Python tips")

    # docstring_from_type_hints(python_tips_dir, overwrite_script=True, test=False)

    structure = automate_mkdocs_from_docstring(
        mkdocs_dir='modules',
        mkgendocs_f='mkgendocs.yml',
        repo_dir=python_tips_dir,
        match_string='pages:\n',
    )

    automate_nav_structure(
        mkdocs_dir='modules',
        mkdocs_f='mkdocs.yml',
        repo_dir=python_tips_dir,
        match_string='- Home: index.md\n',
        structure=structure
    )


if __name__ == '__main__':
    main()
