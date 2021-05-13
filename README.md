# ComplaintsManagement
Customer complaints management regarding sellers, workshops, etc.

The main file performs a set of queries to aquire data from diffent tables in company's main data base.

These tables are compared to a CustomerComplaint xlsx file which contains information about different kind of complaints:

   *Customer attention service
   * Claim adjuster
   * Call center
   * Workshops
    * Attention time
    * Quality of the repair
    * Quality of replacements
   * Etc...
The company runs the main code monthly on demand so there is no need to create an automated task so far.

The main file uses several function from CustomizedComplaints.R, which is a repository of functions that performs repetitive tasks.

This code is based on Tarek Peña, M. Sc's code.
