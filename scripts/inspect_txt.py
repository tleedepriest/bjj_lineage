"""
Want to create some simple graphs to analyze the text
"""
import sys
from pathlib import Path
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
import matplotlib.pyplot as plt
#import bokeh

def get_file_list(path_to_dir):
    """
    return all files in a given directory
    """
    return [x for x in Path(path_to_dir).glob("**/*") if x.is_file()]

def main(path_to_txt_dir):
    nltk.download('punkt')
    nltk.download('stopwords')    
    txt_files = [x for x in Path(path_to_txt_dir).glob("**/*") if x.is_file()]
    stop = stopwords.words('english')
    freqdist = FreqDist()    
    for txt_file in txt_files:
        with txt_file.open() as fh:
            contents = fh.read()
            sentences = sent_tokenize(contents)
            for sentence in sentences:
                print(sentence)
                tokens = word_tokenize(contents)
                no_stop = [token for token in tokens 
                        if token.lower() not in stop]
                for token in no_stop:
                    freqdist[token]+=1

    samples = [item for item, _ in freqdist.most_common(50)]
    freqs = [freqdist[sample] for sample in samples]

    ax = plt.gca() 
    ax.grid(True, color="silver")
    kwargs = {"linewidth": 2,
              "title": "Frequency Distribution"}
    
    if "title" in kwargs:
        ax.set_title(kwargs["title"])
        del kwargs["title"]
    
    ax.plot(freqs, **kwargs)
    ax.set_xticks(range(len(samples)))
    ax.set_xticklabels([str(s) for s in samples], rotation=90)
    ax.set_xlabel("Samples")
    ax.set_ylabel("Counts")
    plt.savefig("generated_data/test.png")

if __name__ == "__main__":
    main(sys.argv[1])
