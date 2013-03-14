# Mitch Andrews
# 11/15/10

# cold.globals.py program dependencies:
#  global imports:
#	sys
#	os


import sys
import os

		
ModuleWDir = os.path.abspath('.')
ModuleLocation = ModuleWDir + "/" + sys.argv[0]
ModulePathAbs = os.path.dirname(ModuleLocation)


def PrintProgramHelp():
	print "-INSERT EXEC HELP HERE-"
