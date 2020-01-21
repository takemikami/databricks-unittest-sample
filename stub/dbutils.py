# Databricks dbutils stub
import glob
import shutil
import os
from typing import Dict, Any

# widgets stub
class widgets:
  widgets_map: Dict[str, Any] = dict()

  @staticmethod
  def text(key_name: str, default_value: Any, description: str) -> None:
    widgets.widgets_map[key_name] = default_value

  @staticmethod
  def get(key_name: str) -> Any:
    return widgets.widgets_map[key_name]

# fs stub
class fs:
  @staticmethod
  def ls(path: str) -> list:
    rtn = list()
    if path.startswith("file:"):
      if not os.path.exists(path[7:]):
        raise FileNotFoundError
      for f in glob.glob(path[7:] + "/*"):
        rtn.append(dbutils_file("file://{}".format(f)))
    else:
      print("skip: dbutils.fs.ls({})".format(path))
    return rtn

  @staticmethod
  def rm(path: str, recurse: bool = False) -> None:
    print(path)
    if path.startswith("file:"):
      print(path[7:])
      shutil.rmtree(path[7:], recurse)
    else:
      print("skip: dbutils.fs.rm({},{})".format(path, recurse))

  @staticmethod
  def mv(path_from: str, path_to: str) -> None:
    if path_from.startswith("file:") and path_to.startswith("file:"):
      if os.path.exists(path_to[7:]):
        shutil.rmtree(path_to[7:], ignore_errors=True)
      shutil.move(path_from[7:], path_to[7:])
    else:
      print("skip: dbutils.fs.mv({},{})".format(path_from, path_to))

class dbutils_file:
  def __init__(self, path: str):
    self.path = path
    self.name = path.split('/')[-1]
    self.isDir = os.path.isdir(path[7:])
    self.isFile = os.path.isfile(path[7:])
