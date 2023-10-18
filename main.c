/*
 * mqremoteput - Demo program to send messages to a remote queue
 * 
 * Summary:
 * 
 * Resources:
 * +IBM MQ Advanced for Developers on ubuntu machine S1558
 *   -Queue Manager:
 *     -QM_S1558
 * 
 *   -Queues:
 *     -DEV.Q1   - Remote queue definition
 *     -QM_E6410 - Transmission queue
 * 
 *   -Channels:
 *     -DEV.APP.SVRCONN   - Server-connection
 *     -QM_S1558.QM_E6410 - Sender
 * 
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cmqc.h>

int main(int argc, char **argv)
{
   MQCNO   cnxOpt = {MQCNO_DEFAULT};               //Connection options  
   MQCSP   cnxSec = {MQCSP_DEFAULT};               //Security parameters
   MQOD    objDsc = {MQOD_DEFAULT};                //Object Descriptor
   MQMD    msgDsc = {MQMD_DEFAULT};                //Message Descriptor
   MQPMO   putOpt = {MQPMO_DEFAULT};               //Put message options
   
   //MQ handles and variables
   MQHCONN  Hcnx;                                  //Connection handle
   MQHOBJ   Hobj;                                  //Object handle 
   MQLONG   opnOpt;                                //MQOPEN options  
   MQLONG   clsOpt;                                //MQCLOSE options 
   MQLONG   cmpCde;                                //MQCONNX completion code 
   MQLONG   opnCde;                                //MQOPEN completion code 
   MQLONG   resCde;                                //Reason code  
   
   //Connection literals/variables
   char *pQmg = "QM_S1558";                        //Target queue manager
   char *pQue = "DEV.Q1";                          //Target queue
   char uid[10];                                   //User ID
   char pwd[10];                                   //User password
   FILE *pFP;                                      //File handle pointer
   
   //-------------------------------------------------------
   //Connect to the queue manager
   //-------------------------------------------------------
   cnxOpt.SecurityParmsPtr = &cnxSec;
   cnxOpt.Version = MQCNO_VERSION_5;
   cnxSec.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
   
   pFP = fopen("/home/adam/mqusers","r");
   if (pFP == NULL){
	   fprintf(stderr, "fopen() failed in file %s at line # %d", __FILE__,__LINE__);
	   return EXIT_FAILURE;
	}
   
	fscanf(pFP,"%s %s",uid,pwd);
	fclose(pFP);
   cnxSec.CSPUserIdPtr = uid;                                            
   cnxSec.CSPUserIdLength = strlen(uid);
   cnxSec.CSPPasswordPtr = pwd;
   cnxSec.CSPPasswordLength = strlen(pwd);
   MQCONNX(pQmg,&cnxOpt,&Hcnx,&cmpCde,&resCde);                            //Queue manager = QM_S1558
   
   if (cmpCde == MQCC_FAILED){
      printf("MQCONNX failed with reason code %d\n",resCde);
      return (int)resCde;
   }
   
   if (cmpCde == MQCC_WARNING){
     printf("MQCONNX generated a warning with reason code %d\n",resCde);
     printf("Continuing...\n");
   }
   
   //-------------------------------------------------------
   //Open DEV.Q1 for output
   //-------------------------------------------------------
   opnOpt = MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING;
   strncpy(objDsc.ObjectName,pQue,strlen(pQue));                                      //Queue = DEV.Q1               
   MQOPEN(Hcnx,&objDsc,opnOpt,&Hobj,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("unable to open %s queue for output\n",pQue);
      printf("Disconnecting from %s and exiting\n",pQmg);
      printf("Press enter to continue\n");
      getchar();
      MQDISC(&Hcnx,&cmpCde,&resCde);
      return((int)opnCde);
   }
   
   //-------------------------------------------------------
   // Put msgs
   //-------------------------------------------------------
   memcpy(msgDsc.Format,MQFMT_STRING,(size_t)MQ_FORMAT_LENGTH);            //Char string fmt
   putOpt.Options = MQPMO_NO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING;
   putOpt.Options |= MQPMO_NEW_MSG_ID;                                     //Unique MQMD.MsgId for each datagram
   putOpt.Options |= MQPMO_NEW_CORREL_ID;
   
   char *msgBuf = "Test from S1558";
   int msgLen = 15;
   MQPUT(Hcnx,Hobj,&msgDsc,&putOpt,msgLen,msgBuf,&cmpCde,&resCde);         //Send to remote queue
   
   if (resCde != MQRC_NONE){
      printf("\nMQPUT ended with reason code %d\n",resCde);
   }
   
   //-------------------------------------------------------
   //Close 
   //-------------------------------------------------------
   clsOpt = MQCO_NONE;
   MQCLOSE(Hcnx,&Hobj,clsOpt,&cmpCde,&resCde);

   if (resCde != MQRC_NONE)
      printf("MQCLOSE ended with reason code %d\n",resCde);
     
   //Disconnect from the queue manager
   MQDISC(&Hcnx,&cmpCde,&resCde);

   if (resCde != MQRC_NONE)
      printf("MQDISC ended with reason code %d\n",resCde);
          
   printf("\nDisconnected from %s\n",pQue);
   return EXIT_SUCCESS;
}