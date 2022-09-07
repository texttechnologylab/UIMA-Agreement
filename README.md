# UIMA-Agreement

UIMA Inter-Annotator Agreement Module for the [TextImager](https://github.com/texttechnologylab/text-)/[TextAnnotator](https://www.texttechnologylab.org/applications/textannotator/) Pipeline

[![Paper](http://img.shields.io/badge/paper-ACL--anthology-B31B1B.svg)](https://aclanthology.org/2020.lrec-1.112/)
[![Conference](http://img.shields.io/badge/conference-LREC--2020-4b44ce.svg)](https://lrec2020.lrec-conf.org/)
[![TextAnnotator](http://img.shields.io/badge/demo-TextAnnotator-378cc3.svg)](http://www.textannotator.texttechnologylab.org/)
[![](https://jitpack.io/v/texttechnologylab/UIMA-Agreement.svg)](https://jitpack.io/#texttechnologylab/UIMA-Agreement)
[![version](https://img.shields.io/github/license/texttechnologylab/UIMA-Agreement)]()
[![latest](https://img.shields.io/github/v/release/texttechnologylab/UIMA-Agreement)]()
## Description

### UIMA Analysis Engines
The engines work with any type system.
The annotation classes to be considered can be white- or blacklisted by (super-)class name.
The CAS view names to be considered can also be white- or blacklisted.
The engine also supports multiple CAS's which can be either processed each by them self or collated into a single annotation study.

### Agreement Measures
The agreement values are computed using [DKPro Agreement (Meyer et al., 2014)](https://dkpro.github.io/dkpro-statistics/) There are UIMA Analysis Engines for both coding and unitizing studies. A total of five different agreement measures can be used:

| Agreement Measure                    | Type      | Raters |
|:-------------------------------------|:----------|:-------|
| Cohen's &kappa; (1960)               | coding    | 2      |
| Percentage agreement                 | coding    | &ge; 2 |
| Fleiss's &kappa; (1971) [multi-&pi;] | coding    | &ge; 2 |
| Krippendorff's &alpha; (1980)        | coding    | &ge; 2 |
| Krippendorff's &alpha;u (1995)       | unitizing | &ge; 2 |

Visit the [DKPro Statistics](https://dkpro.github.io/dkpro-statistics/) website for more information about the agreement measures.

#### Coding Studies
For coding studies each annotation span is split by the covered tokens. There are different stragegies for overlapping annotations available:
- MAX (default): treat all annotations for each author as a set.Compute the cartesian product of each _n_ authors' annotation sets and choose the _n_-tuple with the highest agreement.
- ALL: add all _n_-tuples from the cartesian product of the annotation sets to the study.
- MATCH: take the set of all annotated category across all _n_ authors. Add a _n_-tuple for each category, where the _i_-th authors position is empty, if they did not annotate this specific category.

#### Unitizing Studies
The unitizing study approach does not suffer from problems with overlapping or nested annotations by design.
The only condition is, that for annotation _B_ nested inside another annotation _A_, _B_ may not be of the same category as _A_.

### Annotation
The engines offer different capabilities for creating annotations containing the agreement scores. In either way, the annotations will be created in a special `IAA` view, separate from other annotations.

#### Token-level Annotations
The coding engines can create an agreement score per token if `PARAM_ANNOTATE_TOKEN` is set `true`.

#### Document-level Annotations
Both coding and unitizing engines can create an agreement annotation over an entire document if `PARAM_ANNOTATE_DOCUMENT` is set `true`.

## Citation
IF you want to use the project, please cite:
> Giuseppe Abrami, Manuel Stoeckel, and Alexander Mehler. 2020. TextAnnotator: A UIMA Based Tool for the Simultaneous and Collaborative Annotation of Texts. In Proceedings of the 12th Language Resources and Evaluation Conference, pages 891–900, Marseille, France. European Language Resources Association. ([link](https://aclanthology.org/2020.lrec-1.112/))

### BibTeX
```
@inproceedings{Abrami:et:al:2020,
    title = "{T}ext{A}nnotator: A {UIMA} Based Tool for the Simultaneous and Collaborative Annotation of Texts",
    author = "Abrami, Giuseppe  and
      Stoeckel, Manuel  and
      Mehler, Alexander",
    booktitle = "Proceedings of the 12th Language Resources and Evaluation Conference",
    month = may,
    year = "2020",
    address = "Marseille, France",
    publisher = "European Language Resources Association",
    url = "https://aclanthology.org/2020.lrec-1.112",
    pages = "891--900",
    abstract = "The annotation of texts and other material in the field of digital humanities and Natural Language Processing (NLP) is a common task of research projects. At the same time, the annotation of corpora is certainly the most time- and cost-intensive component in research projects and often requires a high level of expertise according to the research interest. However, for the annotation of texts, a wide range of tools is available, both for automatic and manual annotation. Since the automatic pre-processing methods are not error-free and there is an increasing demand for the generation of training data, also with regard to machine learning, suitable annotation tools are required. This paper defines criteria of flexibility and efficiency of complex annotations for the assessment of existing annotation tools. To extend this list of tools, the paper describes TextAnnotator, a browser-based, multi-annotation system, which has been developed to perform platform-independent multimodal annotations and annotate complex textual structures. The paper illustrates the current state of development of TextAnnotator and demonstrates its ability to evaluate annotation quality (inter-annotator agreement) at runtime. In addition, it will be shown how annotations of different users can be performed simultaneously and collaboratively on the same document from different platforms using UIMA as the basis for annotation.",
    language = "English",
    ISBN = "979-10-95546-34-4",
}
```


## References
> Christian M. Meyer, Margot Mieskes, Christian Stab, and Iryna Gurevych: DKPro Agreement: An Open-Source Java Library for Measuring Inter-Rater Agreement, in: _Proceedings of the 25th International Conference on Computational Linguistics (COLING)_, pp. 105–109, August 2014. Dublin, Ireland. ([download](https://www.ukp.tu-darmstadt.de/publications/details/?no_cache=1&tx_bibtex_pi1%5Bpub_id%5D=TUD-CS-2014-0863))
