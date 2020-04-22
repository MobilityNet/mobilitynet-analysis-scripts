EXP_CONDA_VER=$1

if [ -z $EXP_CONDA_VER ]; then
    echo "Usage: teardown_conda.sh <version>"
else
    INSTALL_PREFIX=$HOME/miniconda-$EXP_CONDA_VER
    rm -rf $INSTALL_PREFIX
fi
