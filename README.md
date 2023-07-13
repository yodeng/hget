## hget

[![PyPI version](https://img.shields.io/pypi/v/hget.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/hget)
[![Page Views Count](https://badges.toozhao.com/badges/01GGP80ERWW0JSCGTHNT8VKAEG/green.svg)](https://badges.toozhao.com/stats/01GGP80ERWW0JSCGTHNT8VKAEG "Get your own page views count badge on badges.toozhao.com")

hget是用于下载文件的命令行软件，支持http和ftp两种下载协议(`http/https/ftp`)，优化亚马逊云对象存储数据下载`(aws s3 cp)`，采用异步协程并发下载，节省线程开销，提高并发量，支持可中断的，随时恢复的下载方式。在网络不好的情况下，可实现下载速度比`wget/axel/aws s3 cp`快100~200倍以上。



#### 1. 依赖

##### 1.1 运行环境

+ linux64
+ python >=3.8



##### 1.2 其他python模块依赖

+ Cython
+ requests
+ aiohttp
+ boto3
+ tqdm



#### 2. 特点

+ `python async`异步协程并发，减少线程开销，支持更多并发量
+ 对单个下载文件进行分块并发下载，充分利用网络IO，同时支持断点续传
+ 有一个`*.ht`的中间文件，记录各分块下载进度和状态，无其他临时文件
+ 程序`autoreload`机制，网络异常或其他程序异常情况下，会自动重置环境并继续下载
+ 多种异常以及信号处理，确保下载状态能准确保存



#### 3. 安装 

> git仓库安装  (for recommend)

```shell
pip3 install git+https://github.com/yodeng/hget.git
```

> Pypi官方源安装

```shell
pip3 install hget -U
```



#### 4. 使用

`hget`支持命令行运行和模块导入运行

##### 4.1 command-line usage

```shell
$ hget -h 
usage: hget [-h] [-o <file>] [--dir <dir>] [-n <int>] [-c <int>] [-t <int>] [-s <str>] [-d] [-q] [-v] [-p <str>] [--proxy-user <str>] [--proxy-password <str>] [--use-proxy-env] [--noreload]
            [--access-key <str>] [--secrets-key <str>]
            <url>

An interruptable and resumable download accelerator supplementary of wget/axel.

positional arguments:
  <url>                 download url, http/https/ftp/s3 support

optional arguments:
  -h, --help            show this help message and exit
  -o <file>, --output <file>
                        output download file
  --dir <dir>           output download directory
  -n <int>, --num <int>
                        the max number of concurrency, default: auto
  -c <int>, --connections <int>
                        the max number of tcp connections for http/https. more tcp connections can speedup, but might be forbidden by url server, default: auto
  -t <int>, --timeout <int>
                        timeout for download, 30s by default
  -s <str>, --max-speed <str>
                        specify maximum speed per second, case-insensitive unit support (K[b], M[b]...), no-limited by default
  -d, --debug           logging debug
  -q, --quiet           suppress all output except error or download success
  -v, --version         show program's version number and exit
  --noreload            tells hget to NOT use the auto-reloader

proxy arguments:
  -p <str>, --proxy <str>
                        proxy url, statswith http/https
  --proxy-user <str>    set USER as proxy username
  --proxy-password <str>
                        set PASS as proxy password
  --use-proxy-env       use HTTP_PROXY or HTTPS_PROXY environment variables for proxy url

aws arguments:
  --access-key <str>    access key if necessary
  --secrets-key <str>   secrets key if necessary
```

命令和参数解释如下:

| 参数             | 描述                                                         |
| ---------------- | ------------------------------------------------------------ |
| \<url\>          | 位置参数，需要下载的网址，以`http/https/ftp/s3`开头          |
| -o/--output      | 下载保存的文件名，默认当前目录下的下载文件                   |
| -n/--num         | 最大的下载并发量，默认根据下载文件大小自动配置。             |
| -c/--connections | http协议下载时，最大的tcp连接数，默认根据下载文件大小自动配置，值越大可加速下载，但有可能连接过多被服务端拒绝连接 |
| -t/--timeout     | 下载连接的最长超时，默认30秒                                 |
| -s/--max-speed   | 每秒最大数据下载量`(bytes)`，默认无限制，支持不区分大小写的单位`K[B]`，`M[B]`等等 |
| -d/--debug       | `debug`模式，更多的`logging`输出                             |
| -q/--quiet       | 禁止除错误外的全部屏幕输出                                   |
| -v/--version     | 打印软件版本并退出                                           |
| -p/--proxy       | 使用的代理url                                                |
| --proxy-user     | 使用的代理用户名                                             |
| --proxy-password | 使用的代理密码                                               |
| --use-proxy-env  | 使用系统环境变量HTTP_PROXY或HTTPS_PROXY environment的值作为代理url |
| --access-key     | 亚马逊云对象存储访问key，`s3`地址生效，没有可以不提供        |
| --secrets-key    | 亚马逊云对象存储私有key，`s3`地址生效，没有可以不提供        |
| --noreload       | 禁止自动重载，当网络异常或程序异常中断情况下，不进行重置并继续下载 |

+ `-c/--connections`： 最大tcp连接数，自动选择即可，如果要配置，建议不要超过500
+ `-n/--num`:  最大并发量，自动选择即可，如果要配置，建议不要超过1000，否则可能会超出系统`ulimit`限制而被杀掉

##### 4.2 python module import usage

> Simple usage

```python
from hget import hget

url="https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz"
outfile="./hg19.fa.gz"

hget(url=url, outfile=outfile, quiet=False)
```

> Parallel usage

```python
from joblib import Parallel, delayed
from hget import hget

urls = ["https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz",
        "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.fa.gz"]

Parallel(n_jobs=2)(delayed(hget)(url) for url in urls)
```

+ `import`调用方式不支持`auto-reload`

#### 5. 测试

以`ucsc`的`hg19`基因组`fasta`文件下载为例：

> hget https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz                   (单线程异步, 下载速度 5Mb/s)

![hget](https://user-images.githubusercontent.com/18365846/184577883-d4fc8304-8137-4edb-acae-b462ac3f6137.png)

> wget https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz                 (下载速度<20kb/s)

![wget](https://user-images.githubusercontent.com/18365846/184577877-dd479cac-8c7f-45aa-ae15-ca119d646111.png)

> axel -n 40 -a https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz       (40个线程，下载速度约200kb/s左右)

![axel](https://user-images.githubusercontent.com/18365846/184577881-357fa27b-d6f1-4ed7-9c11-7004d3085211.png)



#### 6. 说明

+ hget异步下载，对下载做了并发优化处理
+ 由于并发较大，可能会遇到部分网站服务端拒绝连接的情况，通常几分钟后即可恢复，可通过减少TCP连接和并发量参数控制，也可以通过设置最大下载速度控制



#### 7. License

[MIT license](https://github.com/yodeng/hget/blob/master/LICENSE)
