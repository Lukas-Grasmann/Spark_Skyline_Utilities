#!/bin/bash

# This script is responsible for downloading Spark including Skyline Queries form an URL and installing dependencies
# such that it can run on a clean Ubuntu installation.
#
# This program offers the following arguments:
# basic OR install
# - install dependencies and automatically clone the repository
# - automatically rename the cloned repository such that all other arguments are working
#
# maven
# - build Spark found in "Spark_${SPARK_VERSION}" using maven
# sbt OR build
# - build Spark found in "Spark_${SPARK_VERSION}" using sbt (recommended)
# runnable
# - build a runnable from the source
# - similar to the distributions normally downloaded from official Spark sources
#
# jekyll
# - install jekyll and build using jekyll
# - includes compilation of documentation
#
# antlr
# - Install ANTLR for developing the grammar used by Spark SQL
#
# jupyter
# - install JUPYTER
# - modify .bashrc as needed using the commands in the comments associated with this option

SPARK_VERSION="3.1.2"
SPARK_DOWNLOAD_URL="https://github.com/Lukas-Grasmann/Spark_3.1.2_Skyline.git"

cwd=$(pwd)

if [[ $1 == "basic" || $1 == "install" ]]
then

    echo "############################"
    echo "# Downloading Java         #"
    echo "############################"

    sudo apt install openjdk-11-jdk-headless -y
    sudo apt install default-jdk -y
    export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH"
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"

    echo "############################"
    echo "# Downloading dev tools    #"
    echo "############################"

    echo "-------"
    echo "| GIT |"
    echo "-------"

    sudo apt-get install git -y

    echo "-------"
    echo "| CURL |"
    echo "-------"

    sudo apt-get install curl -y


    echo "---------"
    echo "| Scala |"
    echo "---------"

    sudo apt-get install scala -y

    echo "-------"
    echo "| SBT |"
    echo "-------"

    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
    sudo apt-get update
    sudo apt-get install sbt

    echo "---------"
    echo "| Maven |"
    echo "---------"

    sudo apt install maven -y

    echo "------------"
    echo "| R (base) |"
    echo "------------"

    sudo apt install r-base -y


    echo "----------------"
    echo "| R (devtools) |"
    echo "----------------"

    sudo apt-get install -y r-cran-devtools

    echo "----------------"
    echo "| R (packages) |"
    echo "----------------"

    sudo R --vanilla -e 'install.packages("knitr", repos="https://cran.us.r-project.org")'
    sudo R --vanilla -e 'install.packages("rmarkdown", repos="https://cran.us.r-project.org")'

    echo "---------------------"
    echo "| Python setuptools |"
    echo "---------------------"

    sudo apt install python3-setuptools -y

    echo "############################"
    echo "# Downloading Apache Spark #"
    echo "############################"

    git clone --branch "skyline_master" "${SPARK_DOWNLOAD_URL}"

    mv "Spark_${SPARK_VERSION}_Skyline" "spark-${SPARK_VERSION}"

fi

if [[ $1 == "maven" ]]
then

    cd "spark-${SPARK_VERSION}"

    echo "##########################"
    echo "# Building Spark (MAVEN) #"
    echo "##########################"

    export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=1g"
    ./build/mvn -DskipTests clean package

fi

if [[ $1 == "sbt" || $1 == "build" ]]
then

    cd "spark-${SPARK_VERSION}"

    echo "########################"
    echo "# Building Spark (SBT) #"
    echo "########################"

    ./build/sbt package

fi


if [[ $1 == "runnable" ]]
then

    cd "spark-${SPARK_VERSION}"

    echo "###########################"
    echo "# Building runnable Spark #"
    echo "###########################"

    sudo Rscript -e 'install.packages(c("knitr", "devtools", "testthat", "rmarkdown"), repos="https://cloud.r-project.org/")'
    sudo Rscript -e 'devtools::install_version("roxygen2", version = "7.1.1", repos="https://cloud.r-project.org/")'

    sudo Rscript -e 'install.packages(c("e1071", "qpdf"), repos="https://cloud.r-project.org/")'

    ./dev/make-distribution.sh --name skyline-spark --pip --r --tgz -Psparkr -Phive -Phive-thriftserver -Pmesos -Pyarn -Dhadoop.version=3.0.0 -Pkubernetes

fi

if [[ $1 == "jekyll" ]]
then

    echo "#########################"
    echo "# Building using jekyll #"
    echo "#########################"

    sudo apt install python -y
    sudo apt install ruby-full -y

    sudo apt install python3-pip -y

    sudo gem install jekyll jekyll-redirect-from rouge
    sudo gem install jekyll jekyll-redirect-from webrick
    sudo gem install jekyll jekyll-redirect-from pygments.rb

    sudo Rscript -e 'install.packages(c("knitr", "devtools", "testthat", "rmarkdown"), repos="https://cloud.r-project.org/")'
    sudo Rscript -e 'devtools::install_version("roxygen2", version = "7.1.1", repos="https://cloud.r-project.org/")'

    sudo pip install 'sphinx<3.1.0' mkdocs numpy pydata_sphinx_theme ipython nbsphinx numpydoc 'jinja2<3.0.0'

    sudo apt install openjdk-8-jdk-headless -y

    sudo rm -rf /usr/lib/jvm/bin
    sudo ln -s /usr/lib/jvm/java-8-openjdk-amd64/bin /usr/lib/jvm/bin

    cd "spark-${SPARK_VERSION}"
    cd docs
    sudo jekyll build

    sudo rm -rf /usr/lib/jvm/bin
    sudo ln -s /usr/lib/jvm/java-11-openjdk-amd64/bin /usr/lib/jvm/bin

fi


if [[ $1 == "antlr" ]]
then

    cd "spark-${SPARK_VERSION}"

    echo "####################"
    echo "# Installing ANTLR #"
    echo "####################"

    cd /usr/local/lib
    sudo wget https://www.antlr.org/download/antlr-4.9.2-complete.jar
    export CLASSPATH=".:/usr/local/lib/antlr-4.9.2-complete.jar:$CLASSPATH"
    alias antlr4='java -jar /usr/local/lib/antlr-4.9.2-complete.jar'
    alias grun='java org.antlr.v4.gui.TestRig'

fi

if [[ $1 == "jupyter" ]]
then

    echo "######################"
    echo "# Installing JUPYTER #"
    echo "######################"

    pip install jupyter

    # add to .bashrc if needed

    # export SPARK_HOME='/home/lukas/spark/spark-3.1.2'
    # export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
    # export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
    # export PYSPARK_DRIVER_PYTHON="jupyter"
    # export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
    # export PYSPARK_PYTHON=python3
    # export PATH=$SPARK_HOME:$PATH

fi

cd "$cwd"
