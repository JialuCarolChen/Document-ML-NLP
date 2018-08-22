# Document-ML-NLP
A project to analyse, classify and extract information from documents. All documents are converted to html format and stored on MongDB. The document data on MongoDB includes file name, file path, html string, text string, encoded features, text string, tokenized word list, etc. The extracted data from the documents are modelized and stored in MySQL database.

The jupyter notebooks at root contains implementation of the document classification tasks. All the functions and modules built to implement the classification tasks are stored in the ml folder. The nlp folder contains All NLP text mining code. The etl folder stores the code to transform, modelized and load extracted document data to MySQL database.  
