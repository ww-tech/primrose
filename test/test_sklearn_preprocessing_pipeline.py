import pytest

from primrose.pipelines.sklearn_preprocessing_pipeline import SklearnPreprocessingPipeline
from primrose.transformers.sklearn_preprocessing_transformer import SklearnPreprocessingTransformer

def test__instantiate_preprocessor():
    processor = SklearnPreprocessingPipeline._instantiate_preprocessor("preprocessing.StandardScaler", args=None)
    assert isinstance(processor, SklearnPreprocessingTransformer)

    with pytest.raises(Exception) as e:
        SklearnPreprocessingPipeline._instantiate_preprocessor("preprocessing.junk", args=None)
    assert "Preprocessor junk not found in preprocessing module" in str(e)

