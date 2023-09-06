import os

# define the runtime script alias which can be run simply
_script_aliases_ = {
    "spark": os.path.join(os.path.dirname(__file__), "scripts.py"),
}
