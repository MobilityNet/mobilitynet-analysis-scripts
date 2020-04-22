# Reproducible evaluation of data collection using public data #

[![osx-ubuntu-manual-install](https://github.com/MobilityNet/mobilitynet-analysis-scripts/workflows/osx-ubuntu-manual-install/badge.svg)](https://github.com/MobilityNet/mobilitynet-analysis-scripts/actions?query=workflow%3Aosx-ubuntu-manual-install) [![binder-install](https://github.com/MobilityNet/mobilitynet-analysis-scripts/workflows/binder-install/badge.svg)](https://github.com/MobilityNet/mobilitynet-analysis-scripts/actions?query=workflow%3Abinder-install)

This repository contains juypter notebooks for the evaluation of smartphone
app-based data collection. It is designed to be launched with binder
https://mybinder.org/ so that other community members can run their own
analyses without any additional setup.

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/e-mission/e-mission-eval-public-data.git/master)

**WARNING** Launching the binder is ~V..E..R..Y slow~ pretty fast. ~I am not quite sure why, since the [list of packages](environment.yml) is pretty short.~ I always look at the build logs to track the current progress. Sometimes, the build succeeds in creating the image, but gets stuck in the "Launching server" step. If this happens (and it hasn't happened recently) I reload the binder page which seems to speed things up.

If you do find anything interesting, please consider contributing your notebook
so that others can build on it!

**Questions?** Since this repository is part of a larger project, all issues are tracked [in the central docs repository](https://github.com/e-mission/e-mission-docs/issues). If you have a question, [as suggested by the open source guide](https://opensource.guide/how-to-contribute/#communicating-effectively), please file an issue instead of sending an email. Since issues are public, other contributors can try to answer the question and benefit from the answer.

There are many potential ways to interact with the notebooks here. At one extreme, you can do everything using browser UI tools only. At the other, you can use the CLI. And of course, there are tons of other git tools to work with. The instructions here can give you a sense of the options, but feel free to adapt them to your favourite tools.

## Quickstart ##

1. If you want to write your own code, you may want to start with a template that iterates over the existing data model - https://github.com/e-mission/e-mission-eval-public-data/blob/master/Data_exploration_template.ipynb
1. If you want to run existing analyses, you can use the SF Bay Area experiments as a template:
  - Visualizations from the SF Bay area experiments are in the `timeline_*` files (e.g. `timeline_car_scooter_brex_san_jose.ipynb`)

## Running existing notebooks ##

1. **View only:** Notebooks with outputs embedded are in the [`examples_with_outputs`](examples_with_outputs) folder and can be statically viewed directly in nbviewer (e.g. https://nbviewer.jupyter.org/github/e-mission/e-mission-eval-public-data/blob/master/examples_with_outputs/example_visualization_SFBA_first_failed_experiment.ipynb). There are a lot of logs related to data loading and preprocessing at the beginning, so you want to scroll ahead to the results (e.g. https://nbviewer.jupyter.org/github/e-mission/e-mission-eval-public-data/blob/master/examples_with_outputs/example_visualization_SFBA_first_failed_experiment.ipynb#Now-for-the-results-(calibration,-phone-view)!)
1. **Interactive, UI-only:** Launch the repo in binder and clone one of the example notebooks
1. **Interactive, CLI only:**
    1. Fork + clone the repo
    1. Run `setup/setup.sh` to set up the local environment
        1. you may need to install the correct version of miniconda per instructions
    1. Start a local notebook server (`juypter notebook`)

## Contributing ##
You can contribute analysis results (easy) or additional data and experiments
(more complex but potentially more impactful!)

### Contributing analysis results ###
Please ensure that you contribute cleared notebooks to allow the source control
capabilities of GitHub to work well. To clear a notebook, use (Kernel ->
Restart & Clear Output).

1. **UI-only:**
    1. Download the notebook (Download -> ipynb)
    1. Upload the notebook using the GitHub UI (Upload Files, next to Clone or Download)
1. **CLI only:** Follow the [instructions on github](https://help.github.com/en/articles/creating-a-pull-request) - i.e.
    1. Create a new branch (e.g. `$ git checkout -b`)
    1. Commit the new notebook (e.g. `$ git add` and `$ git commit`)
    1. Push and generate pull request 

### Contributing additonal data and experiments ###
Please see the detailed instructions in [the docs](https://github.com/e-mission/e-mission-docs/blob/d312e28295ce5c53e40088388c736048261fc6a7/docs/manage/evaluation_procedure.md).
