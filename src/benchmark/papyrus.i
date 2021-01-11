 %module papyrus
 %{
 /* Includes the header in the wrapper code */
 #include "c_interface.h"
 %}
 
 /* Parse the header file to generate wrappers */
 %include "c_interface.h"
