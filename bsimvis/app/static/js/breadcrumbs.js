// bsimvis/app/static/js/breadcrumbs.js

window.Breadcrumbs = {
    lastRoutingState: null,
    lastRoute: null,

    generate: function (routingState, route) {
        this.lastRoutingState = routingState;
        this.lastRoute = route;
        const { viewKey, collection, params } = routingState;
        const restful = window.parseRestfulPath ? window.parseRestfulPath() : {};
        let segments = [];

        if (viewKey !== 'collections' && viewKey !== 'pools' && (collection || routingState.pool)) {
            const pool = routingState.pool;
            const rawCollection = stripPoolPrefix(collection);
            const prefix = window.location.pathname.startsWith('/pool/') ? 'pool' : 'pools';
            if (pool) {
                segments.push({
                    label: pool,
                    url: `/${prefix}/${encodeURIComponent(pool)}`,
                    icon: 'fa-solid fa-diagram-project'
                });
                if (rawCollection) {
                    segments.push({
                        label: rawCollection,
                        url: `/${prefix}/${encodeURIComponent(pool)}/collections/${encodeURIComponent(rawCollection)}`,
                        icon: 'fa-solid fa-layer-group'
                    });
                }
            } else if (rawCollection) {
                segments.push({
                    label: rawCollection,
                    url: `/collections/${encodeURIComponent(rawCollection)}`,
                    icon: 'fa-solid fa-layer-group'
                });
            }
        }

        switch (viewKey) {
            case 'collections':
                segments.push({
                    label: 'Collections',
                    url: '/collections',
                    icon: 'fa-solid fa-layer-group'
                });
                break;
            case 'batches':
                segments.push({
                    label: route ? route.title : 'Batches',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-boxes-stacked'
                });
                break;
            case 'files':
                segments.push({
                    label: route ? route.title : 'Files',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-file-code'
                });
                break;
            case 'functions':
                segments.push({
                    label: route ? route.title : 'Functions',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-code'
                });
                break;
            case 'features-global':
                segments.push({
                    label: route ? route.title : 'Global Features',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-fingerprint'
                });
                break;
            case 'function-similarity':
                segments.push({
                    label: 'Functions',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['functions']),
                    icon: 'fa-solid fa-code'
                });
                segments.push({
                    label: route ? route.title : 'Similarities',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-code-compare'
                });
                break;
            case 'binary-similarity':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                segments.push({
                    label: route ? route.title : 'Similarities',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-code-compare'
                });
                break;
            case 'clusters':
                segments.push({
                    label: 'Functions',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['functions']),
                    icon: 'fa-solid fa-code'
                });
                segments.push({
                    label: route ? route.title : 'Clusters',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-bullseye'
                });
                break;
            case 'bin-clusters':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                segments.push({
                    label: route ? route.title : 'Clusters',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-bullseye'
                });
                break;
            case 'jobs':
                segments.push({
                    label: route ? route.title : 'Jobs',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-server'
                });
                break;
            case 'upload':
                segments.push({
                    label: route ? route.title : 'Upload',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-cloud-arrow-up'
                });
                break;

            // Module views
            case 'file':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const fmd5Val = params.get('md5') || restful.md5;
                const cachedFileName = window.filenameCache && window.filenameCache[fmd5Val] ? window.filenameCache[fmd5Val] : null;
                segments.push({
                    label: cachedFileName || (fmd5Val ? (fmd5Val.substring(0, 12) + '…') : 'Details'),
                    url: window.location.pathname,
                    icon: 'fa-solid fa-file-code'
                });
                break;
            case 'function':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const funcMd5 = params.get('md5') || restful.md5;
                if (funcMd5) {
                    const cachedFuncFileName = window.filenameCache && window.filenameCache[funcMd5] ? window.filenameCache[funcMd5] : null;
                    segments.push({
                        label: cachedFuncFileName || (funcMd5.substring(0, 12) + '…'),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', funcMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                segments.push({
                    label: params.get('address') ? `Func @${params.get('address')}` : 'Details',
                    url: window.location.pathname,
                    icon: 'fa-solid fa-code'
                });
                break;
            case 'function_features':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const featMd5 = params.get('md5') || restful.md5;
                if (featMd5) {
                    const cachedFeatFileName = window.filenameCache && window.filenameCache[featMd5] ? window.filenameCache[featMd5] : null;
                    segments.push({
                        label: cachedFeatFileName || (featMd5.substring(0, 12) + '…'),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', featMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                if (params.get('md5') && params.get('address')) {
                    segments.push({
                        label: `Func @${params.get('address')}`,
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['function', params.get('md5'), params.get('address')]),
                        icon: 'fa-solid fa-code'
                    });
                }
                segments.push({
                    label: `Features`,
                    url: window.location.pathname,
                    icon: 'fa-solid fa-wand-magic-sparkles'
                });
                break;
            case 'diff':
                const sourceMd5 = restful.md5 || params.get('md5') || params.get('file_md5');
                const sourceAddr = restful.address || params.get('address');
                if (sourceMd5) {
                    const cachedSourceFileName = window.filenameCache && window.filenameCache[sourceMd5] ? window.filenameCache[sourceMd5] : null;
                    segments.push({
                        label: cachedSourceFileName || (sourceMd5.substring(0, 12) + '…'),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', sourceMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                if (sourceMd5 && sourceAddr) {
                    const funcLabel = window.currentFuncName && window.currentFuncId && window.currentFuncId.includes(sourceAddr) ? window.currentFuncName : `Func @${sourceAddr}`;
                    segments.push({
                        label: funcLabel,
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['function', sourceMd5, sourceAddr]),
                        icon: 'fa-solid fa-code'
                    });
                }
                const targetColl = restful.coll_b || collection;
                const targetMd5 = restful.md5_b;
                const targetAddr = restful.addr_b;
                let targetLabel = 'VS';
                if (targetMd5 && targetAddr) {
                    const cachedTargetFileName = window.filenameCache && window.filenameCache[targetMd5] ? window.filenameCache[targetMd5] : null;
                    targetLabel = cachedTargetFileName ? `VS ${cachedTargetFileName} @${targetAddr}` : `VS ${targetMd5.substring(0, 8)}@${targetAddr}`;
                }
                segments.push({
                    label: targetLabel,
                    url: window.location.pathname,
                    icon: 'fa-solid fa-right-left'
                });
                break;
            case 'call_graph':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const cgMd5 = params.get('md5') || restful.md5;
                if (cgMd5) {
                    const cachedCgFileName = window.filenameCache && window.filenameCache[cgMd5] ? window.filenameCache[cgMd5] : null;
                    segments.push({
                        label: cachedCgFileName || (cgMd5.substring(0, 12) + '…'),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', cgMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                segments.push({
                    label: 'Call Graph',
                    url: window.location.pathname,
                    icon: 'fa-solid fa-network-wired'
                });
                break;
            case 'feature':
                segments.push({
                    label: 'Global Features',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['features']),
                    icon: 'fa-solid fa-fingerprint'
                });
                segments.push({
                    label: params.get('hash') ? (params.get('hash').substring(0, 12) + '…') : 'Feature',
                    url: window.location.pathname,
                    icon: 'fa-solid fa-hashtag'
                });
                break;
            case 'bin_sim':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const fmd5 = restful.md5 || params.get('md5');
                if (fmd5) {
                    const cachedBinSimA = window.filenameCache && window.filenameCache[fmd5] ? window.filenameCache[fmd5] : null;
                    segments.push({
                        label: cachedBinSimA || (fmd5.substring(0, 12) + '…'),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', fmd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                const bMd5 = restful.md5_b;
                let bLabel = 'VS';
                if (bMd5) {
                    const cachedBinSimB = window.filenameCache && window.filenameCache[bMd5] ? window.filenameCache[bMd5] : null;
                    bLabel = cachedBinSimB ? `VS ${cachedBinSimB}` : `VS ${bMd5.substring(0, 12)}…`;
                }
                segments.push({
                    label: bLabel,
                    url: window.location.pathname,
                    icon: 'fa-solid fa-right-left'
                });
                break;
        }
        return segments;
    },

    render: function (segments) {
        const container = document.getElementById('breadcrumbs-list');
        if (!container) return;

        if (segments.length <= 1) {
            container.innerHTML = '';
            return;
        }

        let html = '<nav class="breadcrumb" aria-label="breadcrumb">';
        segments.forEach((segment, index) => {
            if (index > 0) {
                html += '<span class="breadcrumb-sep"><i class="fa-solid fa-chevron-right"></i></span>';
            }
            const iconHtml = segment.icon ? `<i class="${segment.icon}"></i>` : '';
            if (index === segments.length - 1) {
                html += `<span class="breadcrumb-item current">${iconHtml}<span>${segment.label}</span></span>`;
            } else {
                html += `<a href="${segment.url}" class="breadcrumb-item" onclick="Nav.openPath('${segment.url}', event)">${iconHtml}<span>${segment.label}</span></a>`;
            }
        });
        html += '</nav>';
        container.innerHTML = html;
    },

    refresh: function () {
        if (this.lastRoutingState) {
            const segments = this.generate(this.lastRoutingState, this.lastRoute);
            this.render(segments);
        }
    }
};
