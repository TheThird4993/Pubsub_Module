#include <linux/module.h>
#include <linux/init.h>
#include <linux/printk.h>
#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/uaccess.h>

#include <linux/list.h>
#include <linux/slab.h>
#include <linux/mutex.h>
#include <linux/moduleparam.h>
#include <linux/proc_fs.h>

#define DEVICE_NAME "pubsub"
#define DEVCOUNT 1

#define SZ_CMD 12
#define SZ_TOPIC 32
#define SZ_MSG 256
#define SZ_BUF 301

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Rodrigo Pacheco");
MODULE_DESCRIPTION("Pseudo MQTT, No sysfs");
MODULE_VERSION("0.2");


static int  temp         = 0;
static bool isOpen       = false;
static int  max_topics   = 10;
static int  topic_counts = 0;

typedef struct SMessage{
	char   content[SZ_MSG];
	struct list_head list;
} SMessage;

typedef struct SProc{
	uint32_t id;
	struct   list_head messages;
	struct 	 list_head list;
} SProc;

typedef struct STopic{
	char     name[SZ_TOPIC];
	uint32_t pub_msgs;
	struct   list_head procs;
	struct   list_head list;
} STopic;

LIST_HEAD(topicList);
DEFINE_MUTEX(pubsubMutex);

module_param(max_topics, int, 0444);

static  dev_t    devno        = 0;
static  struct   cdev chardev = {};

static  struct   class *cls   = NULL;
static  struct   device *dev  = NULL;

static  int	     pubsub_open	  (struct inode*, struct file*);
static  int	     pubsub_release   (struct inode*, struct file*);
static  ssize_t  pubsub_write     (struct file*,  const char __user*, size_t, loff_t*);
static  ssize_t  pubsub_read 	  (struct file*,   	    char __user*, size_t, loff_t*);

static  ssize_t  pubsub_proc_read (struct file*,   	    char __user*, size_t, loff_t*);

static  STopic*  add_topic        (const char*);
static  STopic*  find_topic       (const char*);

static  SProc*   add_proc		  (STopic*, const pid_t);
static  SProc*   find_proc		  (STopic*, const pid_t);
static  void     free_procs       (struct list_head*);

static  uint8_t  pub_message      (STopic*, const char*);

static const struct file_operations fops = {
	.read	 = pubsub_read,
	.write	 = pubsub_write,
	.open	 = pubsub_open,
	.release = pubsub_release
};

static const struct proc_ops pubsub_proc_ops = {
	.proc_read = pubsub_proc_read
};

static int pubsubdrv_init(void){
	pr_info("The \"%s\" driver was inserted.\n", DEVICE_NAME);
	
	// solicita o major number
	int err = alloc_chrdev_region(&devno, 0, DEVCOUNT, DEVICE_NAME);
	if(err){
		pr_alert("Character Device \"%s\" failed to register a major number\n", DEVICE_NAME);
		return err;
	}
	pr_info("Character Device \"%s\" registered", DEVICE_NAME);
	
	// inicializa a estrutura cdev vinculado com as file_operations
	cdev_init(&chardev, &fops);

	// adiciona o driver de caractere ao kernel
	err = cdev_add(&chardev, devno, DEVCOUNT);
	if(err){
		pr_err("Character Device driver failed to add a device\n");
		goto err_cdev;
	}
	pr_info("Character Device successfully added device\n");

	// cria a classe em /sys/class/
	cls = class_create(DEVICE_NAME);
	if(IS_ERR(cls)){
		pr_alert("Character Device failed to register a class\n");
		goto err_cls;
	}
	pr_info("Character Device registered class\n");

	// aciona a criação automática do arquivo em /dev/chardrv
	dev = device_create(cls, NULL, devno, NULL, DEVICE_NAME);
	if(IS_ERR(dev)){
		pr_alert("Character Device failed to create devfs file\n");
		err = PTR_ERR(dev);
		goto err_dev;
	}
	pr_info("Character Device created devfs file\n");

	proc_create(DEVICE_NAME, 0444, NULL, &pubsub_proc_ops);

	return 0;

err_dev:
	class_destroy(cls);
err_cls:
	cdev_del(&chardev);
err_cdev:
	unregister_chrdev_region(devno, DEVCOUNT);

	return err;
}

static void pubsubdrv_exit(void){
	STopic   *t_aux = NULL, *t_tmp = NULL;

	device_destroy(cls, devno);
	class_destroy(cls);
	cdev_del(&chardev);
	unregister_chrdev_region(devno, DEVCOUNT);
	remove_proc_entry(DEVICE_NAME, NULL);

	list_for_each_entry_safe(t_aux, t_tmp, &topicList, list){
		pr_info("Freeing topic[\"%s\"]...\n", t_aux->name);
			free_procs(&t_aux->procs);
		list_del(&t_aux->list);
		kfree(t_aux);	
	}

	pr_info("All done.\n");
	pr_info("The \"%s\" driver was removed...\n", DEVICE_NAME);
}

// ao fazer fopen() ou redirecionar I/O (ex: > /dev/pubsub)
static int pubsub_open(struct inode *inodep, struct file *filep){
	pr_info("Device has been opened %d time(s)\n", ++temp);
	isOpen = true;
	pr_info("The device is in use? %d\n", isOpen);
	return 0;
}

// quando faz fclose() ou o processo morre/encerra
static int pubsub_release(struct inode *inodep, struct file *filep){
	STopic   *t_aux = NULL, *t_tmp = NULL; // Muita redundancia nessa func
	SProc	 *p_aux = NULL, *p_tmp = NULL;
	SMessage *m_aux = NULL, *m_tmp = NULL;

	mutex_lock(&pubsubMutex);

	list_for_each_entry_safe(t_aux, t_tmp, &topicList, list){
		list_for_each_entry_safe(p_aux, p_tmp, &t_aux->procs, list){
			if(p_aux->id == current->pid){
				pr_info("Freeing process[%d]...\n", p_aux->id);
				list_for_each_entry_safe(m_aux, m_tmp, &p_aux->messages, list){
					pr_info("Freeing message [%s]...\n", m_aux->content);
					list_del(&m_aux->list);
					kfree(m_aux);	
				}
				list_del(&p_aux->list);
				kfree(p_aux);
			}
		}
		if(list_empty(&t_aux->procs)){
       		pr_info("No processes subscribed in topic [%s], deleting...\n", t_aux->name);
       		list_del(&t_aux->list);
			kfree(t_aux);
			topic_counts--;
    	}
	}

	pr_info("pubsub successfully closed\n");

	mutex_unlock(&pubsubMutex);

	isOpen = false;
	pr_info("The device is in use? %d\n", isOpen);
	return 0;
}

// quando o espaço de usuário tenta escrever (ex: echo "ex" > /dev/pubsub)
static ssize_t pubsub_write(struct file *filep, const char __user *buffer, size_t len, loff_t *offset){
	char     cmd[SZ_CMD]   = {}, topic[SZ_TOPIC] = {}, message[SZ_MSG] = {};
	char     k_buf[SZ_BUF] = {};
	int      num_params    = 0;
	SMessage *m_aux        = NULL, *tmp = NULL;
	STopic   *t_aux 	   = NULL;
	SProc    *p_aux 	   = NULL;
	//short cmdType     = 0;

	if(len >= (SZ_BUF - 1) ){
		sprintf(message, "(0 characters)");
		pr_alert("Too many characters to deal with (%d)\n", len);
		return 0;
	}

	if(copy_from_user(k_buf, buffer, len)){
		return -EFAULT;
	}

	num_params 		= sscanf(k_buf, "/%15s %31s \"%255[^\"]\"", cmd, topic, message);
	//size_of_string 	= strlen(k_buf);
	//strcpy(tempStr, k_buf);
	pr_info("Received %zu characters from the user\n", len);
	
	pr_info("Received: cmd\t  = %s\n\t  topic\t  = %s\n\t  message = %s\n", cmd, topic, message);

	mutex_lock(&pubsubMutex); // SEÇAO CRITICA =======

	t_aux = find_topic(topic);

	if(!strcmp(cmd, "subscribe") || !strcmp(cmd, "s")){ // ASCII val 115
		pr_info("Subscribe command received!\n");

		if(!t_aux){
			if(topic_counts < max_topics){
				pr_info("Topic not found, creating...\n");
				t_aux = add_topic(topic);
				topic_counts++;
				if(!t_aux){
					pr_err("Not able to create topic.\n");
					len = -ENOMEM;
					goto end;
				}
			}else{
				pr_info("Topic not found, max number of topics reached.\n");
				goto end;
			}
		}

		if (find_proc(t_aux, current->pid)){ 	
			pr_info("Process already subscribed.\n");
			goto end;
		}

		p_aux = add_proc(t_aux, current->pid);

		if(p_aux)
			pr_info("Subscribed!\n");

	} else if(!strcmp(cmd, "unsubscribe") || !strcmp(cmd, "u")){ // ASCII val 117
		pr_info("Unsubscribe command received!\n");

		if(!t_aux){
			pr_info("Topic not found.\n");
			goto end;
		}

		p_aux = find_proc(t_aux, current->pid);

		if(!p_aux){
			pr_info("Process not subscribed in selected topic.\n");
			goto end;
		}

		list_for_each_entry_safe(m_aux, tmp, &p_aux->messages, list){
			list_del(&m_aux->list);
			kfree(m_aux);	
		}

		list_del(&p_aux->list);
		kfree(p_aux);

		if(list_empty(&t_aux->procs)){
        	pr_info("No processes subscribed in topic [%s], deleting...\n", t_aux->name);
        	list_del(&t_aux->list);
			kfree(t_aux);
			topic_counts--;
    	}

		filep->private_data = NULL; 

		pr_info("Process unsubscribed.\n");

	} else if(!strcmp(cmd, "publish") || !strcmp(cmd, "p")){ // ASCII val 112
		pr_info("Publish command received!\n");
		//cmdType = 1;

		if(!t_aux){
			pr_info("Topic not found.\n");
			goto end;
		}
		if(!*message){
			pr_info("Lack of message to publish.\n");
			goto end;
		}

		if(list_empty(&t_aux->procs)){ 
        	pr_info("No processes subscribed.\n");
        	goto end;
    	}

		if(pub_message(t_aux, message)){ 
			pr_err("Failed to publish message to all processes.\n");
			goto end;
		}

	} else if(!strcmp(cmd, "fetch") || !strcmp(cmd, "f")){ // ASCII val 102
		pr_info("Fetch command received!\n");

		if(!t_aux){
			pr_info("Topic not found.\n");
			filep->private_data = NULL;
			goto end;
		}

		p_aux = find_proc(t_aux, current->pid);

		if(!p_aux){
			pr_info("Process not subscribed in selected topic.\n");
			filep->private_data = NULL;
			goto end;
		}

		filep->private_data = t_aux;
		pr_info("Topic [%s] selected to be read.\n", t_aux->name);
		
	} else {
		pr_info("Invalid command received.\n");
		//cmdType = -1;
	}

end:
	mutex_unlock(&pubsubMutex); // FIM DA SEÇAO CRITICA =======
	return len;
}

// quando o espaço de usuário tenta ler do dispositivo (ex: cat /dev/pubsub)
static ssize_t pubsub_read(struct file *filep, char __user *buffer, size_t len, loff_t *offset){
	int      error_count = 0; // ret     = 0, 
	size_t   msg_len = 0;
	SMessage *msg    = NULL;
	SProc    *proc   = NULL;

	//if(*offset > 0) 
	//	return 0;

	if(!filep->private_data){
		pr_info("Private_data is null, Fetch was not realized.\n");
		return 0;
	}

	mutex_lock(&pubsubMutex);

	proc = find_proc(filep->private_data, current->pid);

	if(!proc){
        pr_info("Process not subscribed.\n");
        goto case0;
    }

	if(list_empty(&proc->messages)){
        pr_info("No messages present.\n");
        goto case0;
    }

	msg 	= list_first_entry(&proc->messages, SMessage, list);
	msg_len = strlen(msg->content);

	if(msg_len > len){
		pr_info("Not able to write [%d] characters, writing only [%d] characters\n", msg_len, len);
		msg_len = len;
	}

	error_count = copy_to_user(buffer, msg->content, msg_len);
	if(error_count){
		pr_alert("Failed to send %d characters to the user.\n", error_count);
		mutex_unlock(&pubsubMutex);
		return -EFAULT;
	}

	pr_info("Sent %zu characters to user\n", msg_len);
    //ret     = msg_len;
	//msg_len = 0;

	list_del(&msg->list);
    kfree(msg);

	//*offset += ret; 
	//return ret;
	mutex_unlock(&pubsubMutex);
	return msg_len;

case0:
	mutex_unlock(&pubsubMutex);
	return 0;
}

static ssize_t pubsub_proc_read(struct file *filep, char __user *buffer, size_t len, loff_t *offset){
	int  ret      = 0, error_count = 0, pos = 0;
	STopic *t_aux = NULL;
	char proc_buf[1024]; 

	if(*offset > 0)
		return 0;

	memset(proc_buf, '\0', sizeof(proc_buf));

	mutex_lock(&pubsubMutex);

	list_for_each_entry(t_aux, &topicList, list){
		pos += snprintf(proc_buf + pos, 1024 - pos, "%s: %u\n", t_aux->name, t_aux->pub_msgs);
	}

	error_count = copy_to_user(buffer, proc_buf, pos);
	if(error_count){
		pr_alert("Failed to send %d characters to the /proc.\n", error_count);
		mutex_unlock(&pubsubMutex);
		return -EFAULT;
	}

	ret     = pos;
	*offset += ret;

	mutex_unlock(&pubsubMutex);
	
	return ret;
}

static STopic* add_topic(const char *topicName){
	STopic *topic = kmalloc(sizeof(*topic), GFP_KERNEL);;

	if(!topic){
		pr_err("Failed to create Topic node.\n");
		return NULL;
	}

	INIT_LIST_HEAD(&topic->procs);
	INIT_LIST_HEAD(&topic->list);

	strcpy(topic->name, topicName);
	topic->pub_msgs = 0;

	list_add_tail(&topic->list, &topicList);

	pr_info("Topic \"%s\" created successfully!\n", topicName);

	return topic;
}

static STopic* find_topic(const char *topicName){
	STopic *temp = NULL;

	list_for_each_entry(temp, &topicList, list){
		if(!strcmp(temp->name, topicName))
			return temp;
	}

	return 0;
}

static SProc* add_proc(STopic *topic, const pid_t pid){
	SProc *proc = kmalloc(sizeof(*proc), GFP_KERNEL);

	if(!proc){
		pr_err("Failed to create Proc node.\n");
		return NULL;
	}

	INIT_LIST_HEAD(&proc->messages);
	INIT_LIST_HEAD(&proc->list);

	proc->id = pid;

	list_add_tail(&proc->list, &topic->procs);

	pr_info("Proc id = [%d] added to Topic \"%s\" successfully\n", pid, topic->name);

	return proc;
}

static SProc* find_proc(STopic *topic, const pid_t pid){
	SProc *temp = NULL;

	list_for_each_entry(temp, &topic->procs, list){
		if(temp->id == pid)
			return temp;
	}

	return 0;
}

static uint8_t pub_message(STopic *topic, const char *msg){
	SProc   *p_aux = NULL;
	topic->pub_msgs++;
	uint8_t  yeahBuddy = 0;	// contador de mensagens não escritas em procs

	list_for_each_entry(p_aux, &topic->procs, list){
			SMessage *m_aux = kmalloc(sizeof(*m_aux), GFP_KERNEL);
			
			if(!m_aux){
				pr_err("Failed to create Message node.\n");
				yeahBuddy++;
				continue;
			}

			INIT_LIST_HEAD(&m_aux->list);
			strcpy(m_aux->content, msg);

			list_add_tail(&m_aux->list, &p_aux->messages);
	}

	return yeahBuddy;
}

static void free_procs(struct list_head *list_aux){ //list_aux = t_aux->procs
	SProc	 *p_aux = NULL, *p_tmp = NULL;
	SMessage *m_aux = NULL, *m_tmp = NULL;
	list_for_each_entry_safe(p_aux, p_tmp, list_aux, list){
		pr_info("Freeing process[%d]...\n", p_aux->id);
		list_for_each_entry_safe(m_aux, m_tmp, &p_aux->messages, list){
			pr_info("Freeing message [%s]...\n", m_aux->content);
			list_del(&m_aux->list);
			kfree(m_aux);	
		}
		list_del(&p_aux->list);
		kfree(p_aux);	
	}
}

module_init(pubsubdrv_init);
module_exit(pubsubdrv_exit);

