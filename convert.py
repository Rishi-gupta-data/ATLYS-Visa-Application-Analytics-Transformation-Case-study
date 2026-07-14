"""
Convert Databricks Python notebooks to Jupyter Notebook format (.ipynb)
Handles MAGIC %md (markdown) and %sql cells
"""

import json
import os
import re
from pathlib import Path


def parse_databricks_notebook(filepath):
    """
    Parse a Databricks notebook and extract cells with their types.
    
    Returns:
        list: List of dicts with 'type' and 'content' keys
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by COMMAND separator
    cell_texts = content.split('# COMMAND ----------')
    cells = []
    
    for cell_text in cell_texts:
        cell_text = cell_text.strip()
        if not cell_text:
            continue
        
        # Detect cell type based on content
        lines = cell_text.split('\n')
        cell_type = 'code'
        code_lines = []
        
        for line in lines:
            # Check for MAGIC directives
            if line.startswith('# MAGIC %md'):
                cell_type = 'markdown'
                # Remove the MAGIC directive and continue
                markdown_content = line[len('# MAGIC %md'):].strip()
                if markdown_content:
                    code_lines.append(markdown_content)
            elif line.startswith('# MAGIC %sql'):
                cell_type = 'sql'
                # SQL is typically in a code cell with SQL syntax
                sql_content = line[len('# MAGIC %sql'):].strip()
                if sql_content:
                    code_lines.append(sql_content)
            elif line.startswith('# MAGIC '):
                # Regular MAGIC content (markdown continuation)
                magic_content = line[len('# MAGIC '):].strip()
                code_lines.append(magic_content)
            elif not line.startswith('#'):
                # Regular code/content line
                code_lines.append(line)
        
        content_str = '\n'.join(code_lines).strip()
        
        if content_str:
            cells.append({
                'type': cell_type,
                'source': content_str
            })
    
    return cells


def create_jupyter_notebook(cells):
    """
    Create a Jupyter notebook structure from parsed cells.
    
    Args:
        cells: List of cell dicts with 'type' and 'source'
    
    Returns:
        dict: Jupyter notebook structure
    """
    notebook_cells = []
    
    for cell in cells:
        if cell['type'] == 'markdown':
            # Markdown cell
            nb_cell = {
                'cell_type': 'markdown',
                'metadata': {},
                'source': cell['source'].split('\n') if '\n' in cell['source'] else [cell['source']]
            }
        else:
            # Code cell (includes 'code' and 'sql' types)
            source_lines = cell['source'].split('\n')
            nb_cell = {
                'cell_type': 'code',
                'execution_count': None,
                'metadata': {},
                'outputs': [],
                'source': source_lines
            }
        
        notebook_cells.append(nb_cell)
    
    # Create notebook structure
    notebook = {
        'cells': notebook_cells,
        'metadata': {
            'kernelspec': {
                'display_name': 'Python 3',
                'language': 'python',
                'name': 'python3'
            },
            'language_info': {
                'name': 'python',
                'version': '3.9.0'
            }
        },
        'nbformat': 4,
        'nbformat_minor': 4
    }
    
    return notebook


def convert_file(input_file, output_file=None):
    """
    Convert a Databricks notebook to Jupyter notebook.
    
    Args:
        input_file (str): Path to input .py file
        output_file (str): Path to output .ipynb file (auto-generated if None)
    """
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return False
    
    if output_file is None:
        output_file = input_file.replace('.py', '.ipynb')
    
    try:
        # Parse the Databricks notebook
        cells = parse_databricks_notebook(input_file)
        
        if not cells:
            print(f"⚠️  No cells found in {input_file}")
            return False
        
        # Create Jupyter notebook structure
        notebook = create_jupyter_notebook(cells)
        
        # Write to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Converted: {input_file}")
        print(f"  → {output_file}")
        print(f"  Cells: {len(cells)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error converting {input_file}: {str(e)}")
        return False


def convert_directory(directory, pattern='*.py'):
    """
    Convert all Databricks notebooks in a directory.
    
    Args:
        directory (str): Directory path
        pattern (str): File pattern to match
    """
    directory = Path(directory)
    
    if not directory.exists():
        print(f"❌ Directory not found: {directory}")
        return
    
    py_files = list(directory.glob(pattern))
    
    if not py_files:
        print(f"⚠️  No files matching '{pattern}' found in {directory}")
        return
    
    print(f"\n{'='*80}")
    print(f"Converting Databricks Notebooks to Jupyter Format")
    print(f"{'='*80}\n")
    print(f"Directory: {directory}")
    print(f"Found: {len(py_files)} Python files\n")
    
    successful = 0
    failed = 0
    
    for py_file in sorted(py_files):
        # Skip convert.py itself
        if py_file.name == 'convert.py':
            continue
        
        if convert_file(str(py_file)):
            successful += 1
        else:
            failed += 1
    
    print(f"\n{'='*80}")
    print(f"Conversion Summary")
    print(f"{'='*80}")
    print(f"✓ Successful: {successful}")
    print(f"✗ Failed: {failed}")
    print(f"Total: {successful + failed}\n")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        # Convert specific file or directory
        target = sys.argv[1]
        if os.path.isfile(target):
            convert_file(target)
        elif os.path.isdir(target):
            convert_directory(target)
        else:
            print(f"❌ Path not found: {target}")
    else:
        # Convert all .py files in current directory
        convert_directory('.')
