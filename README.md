# page_rank

implement page rank algorithm using mapreduce

The program will compute the page rank for each web page in the input file

The PageRank.jar file in the src folder can be used in the following way:

hadoop PageRank.jar PageRank.PageRank input_path output_path intermediate_path

  
  -parameters
  
    input_path is the folder containing input file(s)
  
    output_path is the folder containing final output file(s)
  
    intermediate_path is the folder containing all intermediate files generated during the run
  
  -input files format
    
    page_id_1: page1_neighbor1, page1_neighbor2,.....
    
    page_id_2: page2_neighbor1, page2_neighbor2,.....
    .......
    
  -output files format
    
    page_id, page_rank , neighbors
