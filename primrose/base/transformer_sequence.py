"""a simple container for a list of transformers

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""

from primrose.base.transformer import AbstractTransformer

class TransformerSequence():
    """A container for list of transformers"""

    def __init__(self, sequence=[]):
        """init a transformer sequence

        Args:
            sequence (list of transformers): list of transformers

        Returns:
            nothing. Transformers in sequence are added to the internal list

        """
        self.sequence = []
        
        for s in sequence:
            self.add(s)

    def add(self, transformer):
        """add a transformer to the sequence

        Returns:
            nothing. Side effect is to add transformer to the list

        Raises:
            Exception if transformer is not a transformer

        """
        if not isinstance(transformer, AbstractTransformer):
            raise Exception("Transformer needs to extend AbstractTransformer")

        self.sequence.append(transformer)

    def transformers(self):
        """generate: yield each transformer

        Yields:
            yields a transformer from the list

        """
        for transformer in self.sequence:
            yield transformer
