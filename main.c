/*
 * mqremoteput - Demo program to send messages to a remote queue
 * 
 * Summary:
 * Console program to send minute by minute financial indice data to a remote machine via IBM MQ
 * 
 * MQ Configuration:
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
 * ----------------------------------------------------------------------------------------------
 * Date       Author        Description
 * ----------------------------------------------------------------------------------------------
 * 10/16/23   A. Hout       Original source
 * ----------------------------------------------------------------------------------------------
 * 11/02/23   A. Hout       Add exception logic to deal with a queue full scenario
 * ----------------------------------------------------------------------------------------------
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
   
   //File variables
   FILE *pFP;                                      //File handle pointer
   char *pDataLine = NULL;                         //Indicies data line to transmit
   size_t len = 0;                                 //Input to getline(); will allocate as needed
   ssize_t blkLen;                                 //Data Line (block) length returned by getline()
   int msgCnt = 0;                                 //Count of messages sent
   
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
   
	int scnt = fscanf(pFP,"%s %s",uid,pwd);
	fclose(pFP);
   if (scnt < 2){
      puts("Error pulling user credentials");
      return EXIT_FAILURE;
   }
   
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
   strncpy(objDsc.ObjectName,pQue,strlen(pQue)+1);                          //Queue = DEV.Q1; strlen+1 to include the NULL              
   MQOPEN(Hcnx,&objDsc,opnOpt,&Hobj,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("unable to open %s queue for output\n",pQue);
      printf("Disconnecting from %s and exiting\n",pQmg);
      MQDISC(&Hcnx,&cmpCde,&resCde);
      return (int)opnCde;
   }
   
   //-------------------------------------------------------
   // Pull data lines from file and place them on DEV.Q1
   //-------------------------------------------------------
   printf ("Sending Indicie data to %s %s\n", pQue, pQmg);
   memcpy(msgDsc.Format,MQFMT_STRING,(size_t)MQ_FORMAT_LENGTH);            //Char string fmt
   putOpt.Options = MQPMO_NO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING;
   putOpt.Options |= MQPMO_NEW_MSG_ID;                                     //Unique MQMD.MsgId for each datagram
   putOpt.Options |= MQPMO_NEW_CORREL_ID;
   
   pFP = fopen("/home/adam/Codelite-workspace/mqremoteput/Indices.csv","r");
   if (pFP == NULL){
	   fprintf(stderr, "fopen() failed in file %s at line # %d", __FILE__,__LINE__);
      printf("Disconnecting from %s and exiting\n",pQmg);
      MQDISC(&Hcnx,&cmpCde,&resCde);
	   return EXIT_FAILURE;
   }
   
   //Loop thru the dataset
   while ((blkLen = getline(&pDataLine,&len,pFP)) != -1){
      msgCnt++;
      MQPUT(Hcnx,Hobj,&msgDsc,&putOpt,blkLen,pDataLine,&cmpCde,&resCde);    
   
      if (resCde == MQRC_Q_FULL){                                          //RC 2053
         printf("Queue %s is full. Retrying at 1 second intervals...\n",pQue);
         int rcnt = 1;
         do{
            sleep(1);
            MQPUT(Hcnx,Hobj,&msgDsc,&putOpt,blkLen,pDataLine,&cmpCde,&resCde);
            if (rcnt % 10 == 0){
               printf("\r%d retry attempts",rcnt++);
               fflush(stdout);
            }
         }while(resCde == MQRC_Q_FULL);
      }
      if (resCde != MQRC_NONE){
         printf("\nMQPUT ended with reason code %d\n",resCde);
      }
   }
   
   //-------------------------------------------------------
   //Close the input file and queue connection
   //-------------------------------------------------------
   fclose(pFP);
   if (pDataLine)
      free(pDataLine);
      
   clsOpt = MQCO_NONE;
   MQCLOSE(Hcnx,&Hobj,clsOpt,&cmpCde,&resCde);

   if (resCde != MQRC_NONE)
      printf("MQCLOSE ended with reason code %d\n",resCde);
     
   //Disconnect from the queue manager
   MQDISC(&Hcnx,&cmpCde,&resCde);

   if (resCde != MQRC_NONE)
      printf("MQDISC ended with reason code %d\n",resCde);
          
   printf("\n%d records sent\n",msgCnt);
   return EXIT_SUCCESS;
}