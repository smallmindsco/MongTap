## MongTap: A Generative MongoDB Database
### ...wrapped as a stand-alone MCP Server and Claude Desktop extension

LLMs such as Claude are cool, but they are slow. This is not their fault, really, they're just really big. For those of us who work with large amounts of synthetic test data there is a need for something much, much faster. This is that "faster". MongTap is a stand-alone MongoDB "server". You can connect to it with Compass and all the other methods and it looks like any other MongoDB database. However, the collections in MongTap are backed by DataFlood ML models. DataFlood is a very small, lightweight Machine Learning (ML) model format designed specifically for high-performance sythetic test data generation. So, this means that MongTap doesn't store "documents" in the traditional sense. Rather, it creates a Machine Learning model that represents your data using the following mappings w.r.t. the usual CRUD operations:

- Create: Creating a collection instantiates a new DataFlood model

- Insert: This "trains" the model using one or more documents. The DataFlood model will learn the schema exactly and gather statistical information about the data you're "inserting".

- Update: This updates the model, changing the statistics of the DataFlood model rather than updating an individual document.

- Delete: This effectively removes a training sample from a DataFlood model.

### WHY??!

That's probably the most important question. Simply put, you can have infinite rows from a very tiny ML model on disk. DataFlood is very fast, since it only "knows" the data schema it was trained on and doesn't use nerual networks or anything like that it can generate thousands of documents per second on commodity hardware without the need for GPUs. This can be very useful during development and testing, a DataFlood model is like any other code file and can be versioned, edited, and distributed widely without eating up disk space. DataFlood models support "$entropy" and "$seed" (ask Claude and it'll know because these are provided to the model explicitly). $entropy lets you determine how "random" a generated document may be. $seed works like seed values in other ML systems, it is a way to get exactly the same document or set of documents for a specified $entropy value so you can repeatably generate samples for development and testing.

MongTap DOES NOT store each document that is inserted, so you won't get back exactly what you put in. What you get instead is a system that can do much of what you need for development and testing purposes without the overhead. And, perhaps, for other use-cases as well. For instance, a DataFlood model can be great for generating random states for videogames. It can be used as a way to roll up high-velocity data streams, you can build a dashboard directly off of a DataFlood model (so the dashboard updates very quickly) while "training" it via incoming data at the same time. It can be used as a filter to identify outliers and anomolies. It slices! It dices! Alas it cannot blend a bass...


### Human-Editable and Human-Readable ML

A DataFlood ML model is a .json Schema document with statistical information per element. It is stored in .json format and thus anything that can read/edit .json can read/edit a DataFlood model. This allows fine-grained control over how a DataFlood model behaves. You can control co-occurance and do a whole bunch of other things simply by editing the model. Much will be done for you during "training". If you want to go deeper you can easily do so using your preferred .json editor.





