"""
Writing the script from the Luigi Documentation to build some
muscle memory and test
"""
import luigi

# takes in Task class
class GenerateWords(luigi.Task):

    # define what is being made
    def output(self):
        return luigi.LocalTarget('words.text')

    def run(self):

        words = ['some', 'random',
                'words', 'just', 'for',
                'this', 'tutorial']
        with self.output().open('w') as fh:
            for word in words:
                fh.write(f"{word}\n")

# different than above, because this will have a
# dependency unlike Generate Words
class CountLetters(luigi.Task):

    def requires(self):
        return GenerateWords()

    def output(self):
        return luigi.LocalTarget('count_letters.txt')

    def run(self):
        # input returns the target of Generate Words
        with self.input().open('r') as fh:
            words = fh.read().splitlines()

        with self.output().open('w') as fh:
            for word in words:
                fh.write(f"{word} | {len(word)}\n")


if __name__ == "__main__":
    luigi.run()

