#!/usr/bin/env bash

arch=$(uname -m)
conda_download_url="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${arch}.sh"

wget \
        --quiet ${conda_download_url} \
        -O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/miniconda \
    && $HOME/miniconda/bin/conda init \
    && rm /tmp/miniconda.sh \

# The following code from conda init to avoid to renter the shell
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$("$HOME/miniconda/bin/conda" 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "$HOME/miniconda/etc/profile.d/conda.sh" ]; then
        . "$HOME/miniconda/etc/profile.d/conda.sh"
    else
        export PATH="$HOME/miniconda/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
