// random crap

#define NUMMDS 30
#define NUMOSD 10

#define CLIENT_CACHE      100
#define CLIENT_CACHE_MID  .5

#define LOG_MESSAGES

#define LOGGER_INTERVAL 10.0

#define MAX_TRIMMING          16    // max events to be retiring simultaneously
#define LOGSTREAM_READ_INC  4096    // make this bigger than biggest event

#define FAKE_CLOCK

#define NUMCLIENT             1000
#define CLIENT_REQUESTS       100

#define DEBUG_LEVEL 10

#define MDS_CACHE_SIZE        2500
#define MDS_CACHE_MIDPOINT    .8


#define MPI_DEST_TO_RANK(dest,world)    ((dest)<(NUMMDS+NUMOSD) ? \
										 (dest) : \
										 ((NUMMDS+NUMOSD)+(((dest)-NUMMDS-NUMOSD) % (world-NUMMDS-NUMOSD))))
	 
