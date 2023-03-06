# Tweet Turing Test: Detecting disinformation on Twitter

**Capstone Project for Master of Science in Data Science  - Fall 2022 / Winter 2023**  
_College of Computing and Informatics - Drexel University - Philadelphia, PA USA_

**Group Members:**
- John Johnson (jmj382@drexel.edu)
- Katy Matulay (km3868@drexel.edu)
- Justin Minnion (jm4724@drexel.edu)
- Jared Rubin (jar624@drexel.edu)

## About the Project
As our capstone project for our respective Master of Science in Data Science degrees, our team is working to determine ways to detect disinformation on Twitter using machine learning and natual language processing techniques.

### Status
This project is still in active development. Data acquisition and pre-processing was completed during Q4 CY2022, while analysis and modeling is being completed during Q1 CY2023.

## Requirements

### Environment
The environments used for this project have:
- Python 3.7+
- Packages consistent with the included `requirements.txt` file.
- Support for Jupyter notebooks
- For modeling: an attached CUDA-enabled GPU with at least 8GB VRAM

### Packages
The Python packages listed below are used to accomplish various portions of our project. Additional packages from the Python standard library are used but are not listed here.
- [Demoji](https://pypi.org/project/demoji/) (for parsing emoji characters)
- [ftfy](https://pypi.org/project/ftfy/) (for cleaning text)
- NLTK (for natural language processing)
- Numpy (to complement Pandas)
- Pandas (for much of the heavy lifting)
- Pyarrow (for Apache Parquet support)
- [pynvml](https://pypi.org/project/pynvml/) (for monitoring GPU usage during training)
- [regex](https://pypi.org/project/regex/) (for regular expressions)
- [scikit-learn](https://pypi.org/project/scikit-learn/) (for ML utilities)
- Seaborn + Matplotlib (for visualization)
- [Squarify](https://pypi.org/project/squarify/) (for Treemap visualizations)
- [tldextract](https://pypi.org/project/tldextract/) (for parsing URL text)
- [🤗 Transformers](https://pypi.org/project/transformers/) (for fine-tuning and using BERT)
- [tweet-counter](https://github.com/nottrobin/tweet-counter) (for more accurately counting Tweet character lengths)
- [vaderSentiment](https://pypi.org/project/vaderSentiment/) (for sentiment analysis)
- [wordcloud](https://pypi.org/project/wordcloud/) (for word cloud visualizations)

We have setup portions of our code to be run on Google Cloud Platform (GCP) resources like Dataproc.
In support of that, additional Python packages are required when executing code within GCP:
- Google Cloud Client Libraries (Storage, Authentication)
- Spark/PySpark (included with Dataproc)

## Notebook Layout / Project Flow

Our project's code is largely contained within Jupyter notebooks. Each notebook serves a specific purpose, building upon upstream notebooks and passing their progress along to downstream notebooks. The relative layout and overall flow of our project notebooks is summarized in the diagram below.

![A flowchart showing how each notebook connects together](/docs/Notebook_Layout.png)

## License
In accordance with the Twitter API terms of service, we are not permitted to share our acquired data in its raw form.

## Contact
Our group members' email addresses are listed above, and we encourage the use of this GitHub repository's Issues/Discussions areas.

## Acknowledgements
The team would like to acknowledge the following people and organizations for their support in making this project possible.

- Professor Milad Toutounchian, Assistant Teaching Professor - College of Computing and Informatics, Drexel University [(link)](https://drexel.edu/cci/about/directory/T/Toutounchian-Milad/)
- Twitter, the Twitter API, and the API's Academic Research access level
- FiveThirtyEight and Oliver Roeder for the article ["Why We're Sharing 3 Million Russian Troll Tweets"](https://fivethirtyeight.com/features/why-were-sharing-3-million-russian-troll-tweets/) and for publishing the [accompanying dataset](https://github.com/fivethirtyeight/russian-troll-tweets/)
- Clemson University researchers Darren Linvill and Patrick Warren, the [original source of the troll tweet dataset](https://doi.org/10.1080/10584609.2020.1718257)
- The many open-source software contributions that enabled our project

