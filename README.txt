touchstone

touchstone is a simple test tool that can generate documents from wikipedia dumps
and other random methods and then feed those documents into Solr

Running:

Do not use build.xml - it is not supported currently.

Run from your IDE after linking the touchstone project to a compatible Lucene/Solr project.

main method is in com.lucid.touchstone.Main

usage: wikifile numdocs url {numthreads}

wiki files can be found at http://dumps.wikimedia.org/


JMeter:

There is also a small start at some jmeter plans that match the generated data can be poached from in the jmeter directory.