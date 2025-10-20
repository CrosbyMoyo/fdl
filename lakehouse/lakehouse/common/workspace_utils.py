# Databricks notebook source
"""
workspace_utils

This module provides functions to interact with Databricks workspaces using the Databricks SDK. This will use default authentication on the workspace. In otherwords, whichever identity executes the notebook will be authed with the SDK.

It includes:
- Listing notebooks in a specified workspace directory.
- Executing a single notebook with given parameters.
- Executing multiple notebooks in parallel from a specified workspace directory.

Functions:
- list_notebooks_in_directory: Lists all notebooks in a workspace directory.
- execute_notebook: Executes a given notebook in the workspace.
- execute_notebooks_in_directory: Executes all notebooks in a directory in parallel.
"""

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType
from concurrent.futures import ThreadPoolExecutor


def list_notebooks_in_directory(path: str='/Workspace') -> list:
    """ List all the notebooks in a specified workspace directory. """
    client=WorkspaceClient()
    all_files=client.workspace.list(path, recursive=True)
    notebook_files = [file for file in all_files if file.object_type.value == 'NOTEBOOK']
    # dims must run before facts... and d comes before f in the alphabet - so sort by notebook name :-?
    notebook_files.sort(key=lambda file: file.path.split('/')[-1])
    return notebook_files


def execute_notebook(path: str, params: dict={}, timeout: int=600):
    """ Execute a notebook in the Databricks workspace with the specified parameters. """
    logger.log.info(f'Executing {path} with params {params} ... ')
    try:
        dbutils.notebook.run(path, timeout, params)
    except Exception as error:
        logger.log.error(f'Error executing {path}: {error}')
        raise error 


def execute_notebooks_in_directory(path: str, params: dict={}, timeout: int=600, max_workers: int=10, filter: str=None) -> None:
    """ Execute all notebooks in a workspace directory in parallel using multiple threads. """
    abs_path=os.path.abspath(path)

    notebooks=list_notebooks_in_directory(abs_path)

    if filter:
        notebooks=[notebook for notebook in notebooks if filter.lower() in notebook.path.lower()]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures=[executor.submit(execute_notebook, notebook.path, params, timeout) for notebook in notebooks]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                raise e
