# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helper methods and classes for unit tests.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import importlib
import sys
import types
import mock


def _import_dummy_module(module_name):
  """Import dummy modules.

  This method imports a dummy module and all its parents based on module name.
  Modules created by this method are of ModuleType. This is useful when
  packages are not available at runtime and you need to place mock
  implementations of classes in those modules for unit testing purposes (e.g.:
  Airflow runtime modules).

  Args:
    module_name: Full module name to import as a dummy module.

  Returns:
    Newly imported module object.
  """
  packages = module_name.split('.')
  full_name = ''
  package_object = None
  for package_name in packages:
    if full_name:
      full_name += '.'
    full_name += package_name
    if not sys.modules.get(full_name, None):
      new_module = types.ModuleType(full_name)
      sys.modules[full_name] = new_module
    package_object = sys.modules[full_name]
  return package_object


def _import(element, dependencies,
            patch_path=None):
  """Import element with provided dependencies.

  This method recursively injects the provided dependencies, assuming all parent
  modules previous exist.

  Args:
    element: Fully-qualified name of the object to be imported.
    dependencies: List of dependencies. dict with *name*, *mock* and *package*
      entries.
    patch_path: current module path to be patched. Paths are recursively
     patched. If patch is None, it is bootstrapped to the first submodule of
     the first dependency in the list.

  Returns:
    Imported object with dependencies injected.
  """
  # Work with the first element in the list.
  dependency = dependencies[0]
  dependency_full_name = dependency['name']
  dependency_mock = dependency['mock']
  dependency_module = dependency['module']

  # Bootstrap patch_path with the first submodule.
  if patch_path is None:
    patch_path = '.'.join(dependency_full_name.split('.')[:2])

  if (patch_path == dependency_module.__name__ or
      len(dependency_full_name.split('.')) == 2):
    # If we are already at the immediate parent of the dependency to be injected
    # Patch the parent module with the dependency module.
    with mock.patch(patch_path, new=dependency_module, create=True):
      # Patch the dependency with the dependency mock.
      with mock.patch(dependency_full_name, new=dependency_mock, create=True):
        if len(dependencies) > 1:
          # If there are still dependencies to patch, continue recursively.
          new_dependencies = dependencies[1:]
          return _import(element, new_dependencies)
        else:
          # Else, we have finished patching dependencies, import element and
          # return.
          element_module_name = '.'.join(element.split('.')[:-1])
          element_name = element.split('.')[-1]
          element_module = importlib.import_module(element_module_name)
          result = getattr(element_module, element_name)
          return result
  else:
    # Else, continue recursively importing the next parent of the current
    # dependency.
    result = None
    print('patch_path: %s' % (patch_path))
    print('dependency_full_name: %s' % (dependency_full_name))
    with mock.patch(patch_path, create=True):
      next_parent_module = dependency_full_name.split(
          '.')[len(patch_path.split('.'))]
      print('next_parent_module: %s' % (next_parent_module))
      new_patch_path = '.'.join([patch_path, next_parent_module])
      result = _import(element, dependencies, new_patch_path)
    return result


def import_with_mock_dependencies(element_full_name, dependencies):
  """Import class with mock dependencies.

  This method imports the provided element (module, class, method, etc.).
  In contrast with the standard import mechanism, this method injects the
  provided dependencies as mocks before exeuting the import. That way,
  you can import objects whose dependencies are not available, or substitute
  those dependencies with mocks.

  Args:
    element_full_name: fully-qualified name of the element to be imported.
    dependencies: list of dependencies. Each element of the list must be a dict
      containing a *name* and a *mock*, where *name* contains the
      fully-qualified name of the dependency and *mock* contains a mock.Mock
      object to be injected.

  Returns:
    Imported object with dependencies injected.
  """
  # Dependencies' modules might not exist, so we import them as mock modules
  # (if not imported already).
  for dependency in dependencies:
    # Module path of the dependency to be imported.
    module_path = '.'.join(dependency['name'].split('.')[:-1])
    print('module_path: %s' % (module_path))
    dependency['module'] = _import_dummy_module(module_path)
  return _import(element_full_name, dependencies)
