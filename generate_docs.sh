

# one time action
# mkdir docs
# mkdir sphinx
# cd sphinx
# sphinx-quickstart


#copy contents of this folder to parent folder

#cp sphinx/run.sh ./

cp sphinx/Makefile ./

cp sphinx/conf.py ./

#cp sphinx/index.rst ./

rm -rf tmp

sphinx-apidoc -f -P -o tmp/source primrose
sphinx-apidoc -f -P -o tmp/source primrose/base
sphinx-apidoc -f -P -o tmp/source primrose/cleanup
sphinx-apidoc -f -P -o tmp/source primrose/conditionalpath
sphinx-apidoc -f -P -o tmp/source primrose/configuration
sphinx-apidoc -f -P -o tmp/source primrose/dag
sphinx-apidoc -f -P -o tmp/source primrose/dataviz
sphinx-apidoc -f -P -o tmp/source primrose/models
sphinx-apidoc -f -P -o tmp/source primrose/pipelines
sphinx-apidoc -f -P -o tmp/source primrose/readers
sphinx-apidoc -f -P -o tmp/source primrose/transformers
sphinx-apidoc -f -P -o tmp/source primrose/writers

make clean;

make html;

rm -rf docs;

mv _build/html docs/;

mkdir docs/img;

cp img/* docs/img;

touch docs/.nojekyll;

rm Makefile

rm conf.py

rm -rf _build

rm -rf tmp

#git add docs/;
#git commit -m "updating docs"
#git push
