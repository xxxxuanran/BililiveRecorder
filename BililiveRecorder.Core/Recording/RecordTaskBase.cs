using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using BililiveRecorder.Core.Api;
using BililiveRecorder.Core.Config;
using BililiveRecorder.Core.Event;
using BililiveRecorder.Core.Scripting;
using BililiveRecorder.Core.Templating;
using Serilog;
using Timer = System.Timers.Timer;

namespace BililiveRecorder.Core.Recording
{
    internal abstract class RecordTaskBase : IRecordTask
    {
        private const string HttpHeaderAccept = "*/*";
        private const string HttpHeaderOrigin = Api.Http.HttpApiClient.HttpHeaderOrigin;
        private const string HttpHeaderReferer = Api.Http.HttpApiClient.HttpHeaderReferer;
        private const string HttpHeaderUserAgent = Api.Http.HttpApiClient.HttpHeaderUserAgent;

        private const int timer_inverval = 2;
        protected readonly Timer timer = new Timer(1000 * timer_inverval);
        protected readonly Random random = new Random();
        protected readonly CancellationTokenSource cts = new CancellationTokenSource();
        protected readonly CancellationToken ct;

        protected readonly IRoom room;
        protected readonly ILogger logger;
        protected readonly IApiClient apiClient;
        private readonly FileNameGenerator fileNameGenerator;
        private readonly UserScriptRunner userScriptRunner;

        private int partIndex = 0;

        protected string? streamHost;
        protected string? streamHostFull;
        protected bool started = false;
        protected bool timeoutTriggered = false;
        protected int qn;

        private readonly object ioStatsLock = new();
        protected int ioNetworkDownloadedBytes;

        protected Stopwatch ioDiskStopwatch = new();
        protected object ioDiskStatsLock = new();
        protected TimeSpan ioDiskWriteDuration;
        protected int ioDiskWrittenBytes;

        private DateTimeOffset ioStatsLastTrigger;
        private TimeSpan durationSinceNoDataReceived;

        protected RecordTaskBase(IRoom room, ILogger logger, IApiClient apiClient, UserScriptRunner userScriptRunner)
        {
            this.room = room ?? throw new ArgumentNullException(nameof(room));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
            this.userScriptRunner = userScriptRunner ?? throw new ArgumentNullException(nameof(userScriptRunner));

            this.fileNameGenerator = new FileNameGenerator(room.RoomConfig, logger);
            this.ct = this.cts.Token;

            this.timer.Elapsed += this.Timer_Elapsed_TriggerIOStats;
        }

        public Guid SessionId { get; } = Guid.NewGuid();

        #region Events

        public event EventHandler<IOStatsEventArgs>? IOStats;
        public event EventHandler<RecordingStatsEventArgs>? RecordingStats;
        public event EventHandler<RecordFileOpeningEventArgs>? RecordFileOpening;
        public event EventHandler<RecordFileClosedEventArgs>? RecordFileClosed;
        public event EventHandler? RecordSessionEnded;

        protected void OnIOStats(IOStatsEventArgs e) => IOStats?.Invoke(this, e);
        protected void OnRecordingStats(RecordingStatsEventArgs e) => RecordingStats?.Invoke(this, e);
        protected void OnRecordFileOpening(RecordFileOpeningEventArgs e) => RecordFileOpening?.Invoke(this, e);
        protected void OnRecordFileClosed(RecordFileClosedEventArgs e) => RecordFileClosed?.Invoke(this, e);
        protected void OnRecordSessionEnded(EventArgs e) => RecordSessionEnded?.Invoke(this, e);

        #endregion

        public virtual void RequestStop() => this.cts.Cancel();

        public virtual void SplitOutput() { }

        public async virtual Task StartAsync()
        {
            if (this.started)
                throw new InvalidOperationException("Only one StartAsync call allowed per instance.");
            this.started = true;

            var (fullUrl, codecQn) = await this.FetchStreamUrlAsync(this.room.RoomConfig.RoomId).ConfigureAwait(false);

            this.qn = codecQn.Qn;
            this.streamHost = new Uri(fullUrl).Host;
            var qnDesc = StreamQualityNumber.MapToString(codecQn.Qn);

            this.logger.Information("连接直播服务器 {Host} 录制画质 {Qn} ({QnDescription})", this.streamHost, codecQn, qnDesc);
            this.logger.Debug("直播流地址 {Url}", fullUrl);

            var stream = await this.GetStreamAsync(fullUrl: fullUrl, timeout: (int)this.room.RoomConfig.TimingStreamConnect).ConfigureAwait(false);

            this.ioStatsLastTrigger = DateTimeOffset.UtcNow;
            this.durationSinceNoDataReceived = TimeSpan.Zero;

            this.ct.Register(state => _ = Task.Run(async () =>
            {
                try
                {
                    if (state is not WeakReference<Stream> weakRef)
                        return;

                    await Task.Delay(1000);

                    if (weakRef.TryGetTarget(out var weakStream))
                    {
#if NET6_0_OR_GREATER
                        await weakStream.DisposeAsync();
#else
                        weakStream.Dispose();
#endif
                    }
                }
                catch (Exception)
                { }
            }), state: new WeakReference<Stream>(stream), useSynchronizationContext: false);

            this.StartRecordingLoop(stream);
        }

        protected abstract void StartRecordingLoop(Stream stream);

        private void Timer_Elapsed_TriggerIOStats(object? sender, ElapsedEventArgs e)
        {
            int networkDownloadBytes, diskWriteBytes;
            TimeSpan durationDiff, diskWriteDuration;
            DateTimeOffset startTime, endTime;


            lock (this.ioStatsLock) // 锁 timer elapsed 事件本身防止并行运行
            {
                // networks
                networkDownloadBytes = Interlocked.Exchange(ref this.ioNetworkDownloadedBytes, 0); // 锁网络统计
                endTime = DateTimeOffset.UtcNow;
                startTime = this.ioStatsLastTrigger;
                this.ioStatsLastTrigger = endTime;
                durationDiff = endTime - startTime;

                this.durationSinceNoDataReceived = networkDownloadBytes > 0 ? TimeSpan.Zero : this.durationSinceNoDataReceived + durationDiff;

                // disks
                lock (this.ioDiskStatsLock) // 锁硬盘统计
                {
                    diskWriteDuration = this.ioDiskWriteDuration;
                    diskWriteBytes = this.ioDiskWrittenBytes;
                    this.ioDiskWriteDuration = TimeSpan.Zero;
                    this.ioDiskWrittenBytes = 0;
                }
            }

            var netMbps = networkDownloadBytes * (8d / 1024d / 1024d) / durationDiff.TotalSeconds;
            var diskMBps = diskWriteBytes / (1024d * 1024d) / diskWriteDuration.TotalSeconds;

            this.OnIOStats(new IOStatsEventArgs
            {
                StreamHost = this.streamHost,
                NetworkBytesDownloaded = networkDownloadBytes,
                Duration = durationDiff,
                StartTime = startTime,
                EndTime = endTime,
                NetworkMbps = netMbps,
                DiskBytesWritten = diskWriteBytes,
                DiskWriteDuration = diskWriteDuration,
                DiskMBps = diskMBps,
            });

            if ((!this.timeoutTriggered) && (this.durationSinceNoDataReceived.TotalMilliseconds > this.room.RoomConfig.TimingWatchdogTimeout))
            {
                this.timeoutTriggered = true;
                this.logger.Warning("检测到录制卡住，可能是网络或硬盘原因，将会主动断开连接");
                this.RequestStop();
            }
        }

        protected (string fullPath, string relativePath) CreateFileName()
        {
            this.partIndex++;

            var output = this.fileNameGenerator.CreateFilePath(new FileNameTemplateContext
            {
                Name = FileNameGenerator.RemoveInvalidFileName(this.room.Name, ignore_slash: false),
                Title = FileNameGenerator.RemoveInvalidFileName(this.room.Title, ignore_slash: false),
                RoomId = this.room.RoomConfig.RoomId,
                ShortId = this.room.ShortId,
                Uid = this.room.Uid,
                AreaParent = FileNameGenerator.RemoveInvalidFileName(this.room.AreaNameParent, ignore_slash: false),
                AreaChild = FileNameGenerator.RemoveInvalidFileName(this.room.AreaNameChild, ignore_slash: false),
                PartIndex = this.partIndex,
                Qn = this.qn,
                Json = this.room.RawBilibiliApiJsonData,
            });

            return (output.FullPath!, output.RelativePath);
        }

        #region Api Requests

        private HttpClient CreateHttpClient()
        {
            var httpClient = new HttpClient(new HttpClientHandler
            {
                AllowAutoRedirect = false,
                UseProxy = this.room.RoomConfig.NetworkTransportUseSystemProxy,
            });
            var headers = httpClient.DefaultRequestHeaders;
            headers.Add("Accept", HttpHeaderAccept);
            headers.Add("Origin", HttpHeaderOrigin);
            headers.Add("Referer", HttpHeaderReferer);
            headers.Add("User-Agent", HttpHeaderUserAgent);
            return httpClient;
        }

        internal static readonly char[] QnParseSeparator = new[] { ',', '，', '、', ' ' };
        private static IReadOnlyList<StreamCodecQn> ParseAllowedQn(string? allowedQn)
        {
            if (string.IsNullOrWhiteSpace(allowedQn)) return Array.Empty<StreamCodecQn>();

            var qns = allowedQn!.Split(QnParseSeparator, StringSplitOptions.RemoveEmptyEntries)
                .Select(static x =>
                {
                    if (int.TryParse(x, out var num))
                    {
                        return new StreamCodecQn
                        {
                            Qn = num,
                            Codec = StreamCodec.AVC
                        };
                    }
                    else if (x.StartsWith("avc", StringComparison.OrdinalIgnoreCase))
                    {
                        if (int.TryParse(x[3..], out num))
                        {
                            return new StreamCodecQn
                            {
                                Qn = num,
                                Codec = StreamCodec.AVC
                            };
                        }
                    }
                    else if (x.StartsWith("hevc", StringComparison.OrdinalIgnoreCase))
                    {
                        if (int.TryParse(x[4..], out num))
                        {
                            return new StreamCodecQn
                            {
                                Qn = num,
                                Codec = StreamCodec.HEVC
                            };
                        }
                    }

                    // invalid
                    return new StreamCodecQn
                    {
                        Qn = -1,
                        Codec = StreamCodec.AVC
                    };
                })
                .Where(x => x.Qn >= 0)
                .ToList();

            return qns;
        }


        private static readonly Regex CdnRegex = new(@"[?&]cdn=([^&]+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static string? ExtractCdnFromExtra(string? extra)
        {
            if (string.IsNullOrEmpty(extra)) return null;

            // 使用正则表达式匹配 ?cdn=xxx 或 &cdn=xxx，忽略大小写
            var match = CdnRegex.Match(extra);

            return match.Success ? match.Groups[1].Value : null;
        }

        private Api.Model.RoomPlayInfo.UrlInfoItem? SelectCdnByConfiguration(Api.Model.RoomPlayInfo.UrlInfoItem[] candidates, string recordingCdn)
        {
            var cdnList = recordingCdn.Split(',')
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToArray();

            if (cdnList.Length == 0) return null;

            // 预先计算所有候选项的 CDN 名称，避免重复计算
            var candidatesWithCdn = candidates
                .Select(x => new { UrlInfo = x, CdnName = ExtractCdnFromExtra(x.Extra) })
                .Where(x => !string.IsNullOrEmpty(x.CdnName))
                .ToArray();
            var availableCdns = candidatesWithCdn.Select(x => x.CdnName).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            // 按配置的 CDN 顺序查找匹配项
            var matched = cdnList
                .Select(configCdn => candidatesWithCdn.FirstOrDefault(x =>
                    string.Equals(x.CdnName, configCdn, StringComparison.OrdinalIgnoreCase)))
                .FirstOrDefault(x => x != null);

            if (matched != null)
            {
                this.logger.Information("匹配 CDN 匹配成功。当前CDN {Cdn}, 可用CDN {Available}",
                    matched.CdnName);
                return matched.UrlInfo;
            }

            // 记录未匹配的情况
            this.logger.Warning("没有符合设置要求的 CDN，保持随机选择。设置CDN {CdnList}, 可用CDN {Available}",
                cdnList, availableCdns);

            return null;
        }

        protected async Task<(string url, StreamCodecQn codecQn)> FetchStreamUrlAsync(int roomid)
        {
            var allowedQn = ParseAllowedQn(this.room.RoomConfig.RecordingQuality);

            // 优先使用用户脚本获取直播流地址
            if (this.userScriptRunner.CallOnFetchStreamUrl(this.logger, roomid, allowedQn) is { } urlFromScript)
            {
                this.logger.Information("使用用户脚本返回的直播流地址 {Url}", urlFromScript);
                return (urlFromScript, new StreamCodecQn { Codec = StreamCodec.AVC, Qn = -1 });
            }

            const int DefaultQn = 25000;
            var codecItems = await this.apiClient.GetCodecItemInStreamUrlAsync(roomid: roomid, qn: DefaultQn).ConfigureAwait(false);
            //?? throw new Exception("no supported stream url, qn: " + DefaultQn);

            var allAvailableCodecQn = new List<StreamCodecQn>();

            if (codecItems.avc is not null)
            {
                allAvailableCodecQn.AddRange(codecItems.avc.AcceptQn.Select(x => new StreamCodecQn
                {
                    Codec = StreamCodec.AVC,
                    Qn = x
                }));
            }
            if (codecItems.hevc is not null)
            {
                allAvailableCodecQn.AddRange(codecItems.hevc.AcceptQn.Select(x => new StreamCodecQn
                {
                    Codec = StreamCodec.HEVC,
                    Qn = x
                }));
            }

            StreamCodecQn selectedCodecQn;
            // Select first avaiable qn
            foreach (var qn in allowedQn)
            {
                if (allAvailableCodecQn.Contains(qn))
                {
                    selectedCodecQn = qn;
                    goto match_qn_success;
                }
            }

            this.logger.Information("没有符合设置要求的画质，稍后再试。设置画质 {QnSettings}, 可用画质 {AcceptQn}", allowedQn, allAvailableCodecQn);
            throw new NoMatchingQnValueException();

        match_qn_success:
            this.logger.Debug("设置画质 {QnSettings}, 可用画质 {AcceptQn}, 最终选择 {SelectedQn}", allowedQn, allAvailableCodecQn, selectedCodecQn);

            if (selectedCodecQn.Qn != DefaultQn)
            {
                // 最终选择的 qn 与默认不同，需要重新请求一次
                codecItems = await this.apiClient.GetCodecItemInStreamUrlAsync(roomid: roomid, qn: selectedCodecQn.Qn).ConfigureAwait(false);
            }

            var item = selectedCodecQn.Codec switch
            {
                StreamCodec.AVC => codecItems.avc,
                StreamCodec.HEVC => codecItems.hevc,
                _ => throw new Exception("unknown codec")
            };

            if (item is null)
                throw new Exception("no supported stream url for " + selectedCodecQn);

            if (item.CurrentQn != selectedCodecQn.Qn)
                this.logger.Warning("返回的直播流地址的画质是 {CurrentQn} 而不是请求的 {SelectedQn}", item.CurrentQn, selectedCodecQn);

            var url_infos = item.UrlInfos;
            if (url_infos is null || url_infos.Length == 0)
                throw new Exception("no url_info");

            // https:// xy0x0x0x0xy.mcdn.bilivideo.cn:486
            var url_infos_without_mcdn = url_infos.Where(x => !x.Host.Contains(".mcdn.")).ToArray();

            // Prefer non-mcdn hosts if available
            var candidates = url_infos_without_mcdn.Length != 0 ? url_infos_without_mcdn : url_infos;

            // 1. 默认先随机选择 CDN，并提取 cdn name
            var url_info = candidates[this.random.Next(candidates.Length)];
            var cdnName = ExtractCdnFromExtra(url_info.Extra);

            // 2. 检查 RecordingCdn 是否不为空，如果不为空则运行 CDN 选择
            var recordingCdn = this.room.RoomConfig.RecordingCdn;
            if (!string.IsNullOrWhiteSpace(recordingCdn))
            {
                var selectedUrlInfo = this.SelectCdnByConfiguration(candidates, recordingCdn!);
                if (selectedUrlInfo != null)
                {
                    url_info = selectedUrlInfo;
                    cdnName = ExtractCdnFromExtra(url_info.Extra);
                }
            }

            // 3. 检查当前 CDN NAME 是否全等于 cn-gotcha01（忽略大小写）
            if (cdnName is not null && string.Equals(cdnName, "cn-gotcha01", StringComparison.OrdinalIgnoreCase))
            {
                // 如果是 cn-gotcha01，检查自定义CN01SID是否不为空
                var customCn01Sid = this.room.RoomConfig.CustomCn01Sid;
                if (!string.IsNullOrWhiteSpace(customCn01Sid))
                {
                    var sidList = customCn01Sid!.Split(',').Select(s => s.Trim()).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
                    if (sidList.Length > 0)
                    {
                        var sid = sidList[this.random.Next(sidList.Length)];
                        var newHost = $"https://{sid}.bilivideo.com";
                        this.logger.Information("已使用 SID {Sid} 替换 {OldHost} 为 {NewHost}", sid, url_info.Host, newHost);
                        url_info.Host = newHost;
                    }
                }
            }
            else
            {
                // 替换Host里 b.bilivideo.com 为 .bilivideo.com 以解决 CDN 劣化
                if (url_info.Host.Contains("b.bilivideo.com"))
                {
                    var newHost = url_info.Host.Replace("b.bilivideo.com", ".bilivideo.com");
                    this.logger.Information("已修正 {OldHost} 为 {NewHost}", url_info.Host, newHost);
                    url_info.Host = newHost;
                }
            }

            var fullUrl = url_info.Host + item.BaseUrl + url_info.Extra;

            return (fullUrl, new StreamCodecQn { Codec = selectedCodecQn.Codec, Qn = item.CurrentQn });
        }

        protected async Task<Stream> GetStreamAsync(string fullUrl, int timeout)
        {
            var client = this.CreateHttpClient();

            var streamHostInfoBuilder = new StringBuilder();

            while (true)
            {
                var allowedAddressFamily = this.room.RoomConfig.NetworkTransportAllowedAddressFamily;
                HttpRequestMessage request;
                Uri originalUri;

                if (this.userScriptRunner.CallOnTransformStreamUrl(this.logger, fullUrl) is { } scriptResult)
                {
                    var (scriptUrl, scriptIp) = scriptResult;

                    this.logger.Debug("用户脚本重定向了直播流地址 {NewUrl}, 旧地址 {OldUrl}", scriptUrl, fullUrl);

                    fullUrl = scriptUrl;
                    originalUri = new Uri(fullUrl);


                    if (scriptIp is not null)
                    {
                        this.logger.Debug("用户脚本指定了服务器 IP {IP}", scriptIp);

                        var uri = new Uri(fullUrl);
                        var builder = new UriBuilder(uri)
                        {
                            Host = scriptIp
                        };

                        request = new HttpRequestMessage(HttpMethod.Get, builder.Uri);
                        request.Headers.Host = uri.IsDefaultPort ? uri.Host : uri.Host + ":" + uri.Port;

                        streamHostInfoBuilder.Append(originalUri.Host);
                        streamHostInfoBuilder.Append(" [");
                        streamHostInfoBuilder.Append(scriptIp);
                        streamHostInfoBuilder.Append(']');

                        goto sendRequest;
                    }
                }
                else
                {
                    originalUri = new Uri(fullUrl);
                }

                if (allowedAddressFamily == AllowedAddressFamily.System)
                {
                    this.logger.Debug("NetworkTransportAllowedAddressFamily is System");
                    request = new HttpRequestMessage(HttpMethod.Get, originalUri);

                    streamHostInfoBuilder.Append(originalUri.Host);
                }
                else
                {
                    var ips = await Dns.GetHostAddressesAsync(originalUri.DnsSafeHost);

                    var filtered = ips.Where(x => allowedAddressFamily switch
                    {
                        AllowedAddressFamily.Ipv4 => x.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork,
                        AllowedAddressFamily.Ipv6 => x.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6,
                        AllowedAddressFamily.Any => true,
                        _ => false
                    }).ToArray();

                    var selected = filtered[this.random.Next(filtered.Length)];

                    this.logger.Debug("指定直播服务器地址 {DnsHost}: {SelectedIp}, Allowed: {AllowedAddressFamily}, {IPAddresses}", originalUri.DnsSafeHost, selected, allowedAddressFamily, ips);

                    streamHostInfoBuilder.Append(originalUri.Host);
                    streamHostInfoBuilder.Append(" [");
                    streamHostInfoBuilder.Append(selected);
                    streamHostInfoBuilder.Append(']');

                    if (selected is null)
                    {
                        throw new Exception("DNS 没有返回符合要求的 IP 地址");
                    }

                    var builder = new UriBuilder(originalUri)
                    {
                        Host = selected.ToString()
                    };

                    request = new HttpRequestMessage(HttpMethod.Get, builder.Uri);
                    request.Headers.Host = originalUri.IsDefaultPort ? originalUri.Host : originalUri.Host + ":" + originalUri.Port;
                }

            sendRequest:

                var resp = await client.SendAsync(request,
                     HttpCompletionOption.ResponseHeadersRead,
                     new CancellationTokenSource(timeout).Token)
                     .ConfigureAwait(false);

                switch (resp.StatusCode)
                {
                    case HttpStatusCode.OK:
                        {
                            this.logger.Information("开始接收直播流");
                            this.streamHostFull = streamHostInfoBuilder.ToString();
                            var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
                            return stream;
                        }
                    case HttpStatusCode.Moved:
                    case HttpStatusCode.Redirect:
                        {
                            fullUrl = new Uri(originalUri, resp.Headers.Location!).ToString();
                            this.logger.Debug("跳转到 {Url}, 原文本 {Location}", fullUrl, resp.Headers.Location!.OriginalString);
                            resp.Dispose();
                            streamHostInfoBuilder.Append('\n');
                            break;
                        }
                    default:
                        throw new Exception(string.Format("尝试下载直播流时服务器返回了 ({0}){1}", resp.StatusCode, resp.ReasonPhrase));
                }
            }
        }
        #endregion
    }
}
