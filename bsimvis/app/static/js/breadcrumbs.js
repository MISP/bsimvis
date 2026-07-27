// bsimvis/app/static/js/breadcrumbs.js

window.Breadcrumbs = {
    lastRoutingState: null,
    lastRoute: null,

    poolNameCache: { __loading: {} },
    funcNameCache: {},

    setPoolName: function(id, name) {
        this.poolNameCache[id] = name;
    },
    setFilename: function(md5, name) {
        window.filenameCache = window.filenameCache || {};
        window.filenameCache[md5] = name;
    },
    setFuncName: function(coll, md5, addr, name) {
        this.funcNameCache[`${coll}:${md5}:${addr}`] = name;
    },
    getPoolLabel: function(id) {
        return this.poolNameCache[id] || 'Pool';
    },
    getFileLabel: function(md5) {
        return window.filenameCache?.[md5] || 'File';
    },
    getFuncLabel: function(coll, md5, addr) {
        return this.funcNameCache[`${coll}:${md5}:${addr}`] || 'Function';
    },
    ensurePoolName: async function(id) {
        if (this.poolNameCache[id]) return;
        if (this.poolNameCache.__loading[id]) return;
        try {
            const r = await fetch(`/api/pool/${encodeURIComponent(id)}`);
            if (!r.ok) return;
            const d = await r.json();
            this.setPoolName(id, d.name || id);
        } catch(e) {}
        delete this.poolNameCache.__loading[id];
    },

    generate: function (routingState, route) {
        this.lastRoutingState = routingState;
        this.lastRoute = route;
        const { viewKey, collection, params } = routingState;
        const restful = window.parseRestfulPath ? window.parseRestfulPath() : {};
        let segments = [];

        if (viewKey !== 'collections' && viewKey !== 'pools' && viewKey !== 'pool-detail' && viewKey !== 'collection-detail' && (collection || routingState.pool)) {
            const pool = routingState.pool;
            const rawCollection = stripPoolPrefix(collection);
            const prefix = window.location.pathname.startsWith('/pool/') ? 'pool' : 'pools';
            if (pool) {
                segments.push({
                    label: 'Pools',
                    url: '/pools',
                    icon: 'fa-solid fa-diagram-project'
                });
                this.ensurePoolName(pool);
                segments.push({
                    label: this.getPoolLabel(pool),
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
                    label: 'Collections',
                    url: '/collections',
                    icon: 'fa-solid fa-layer-group'
                });
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
            case 'pools':
                segments.push({
                    label: 'Pools',
                    url: '/pools',
                    icon: 'fa-solid fa-diagram-project'
                });
                break;
            case 'pool-detail':
                segments.push({
                    label: 'Pools',
                    url: '/pools',
                    icon: 'fa-solid fa-diagram-project'
                });
                if (routingState.pool) {
                    this.ensurePoolName(routingState.pool);
                    segments.push({
                        label: this.getPoolLabel(routingState.pool),
                        url: `/pools/${encodeURIComponent(routingState.pool)}`,
                        icon: 'fa-solid fa-diagram-project'
                    });
                }
                break;
            case 'collection-detail':
                segments.push({
                    label: 'Collections',
                    url: '/collections',
                    icon: 'fa-solid fa-layer-group'
                });
                if (collection) {
                    segments.push({
                        label: stripPoolPrefix(collection),
                        url: `/collections/${encodeURIComponent(collection)}`,
                        icon: 'fa-solid fa-layer-group'
                    });
                }
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
                segments.push({
                    label: this.getFileLabel(fmd5Val),
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
                    segments.push({
                        label: this.getFileLabel(funcMd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', funcMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                segments.push({
                    label: 'Functions',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', funcMd5, 'functions']),
                    icon: 'fa-solid fa-code'
                });
                const funcAddr = params.get('address');
                segments.push({
                    label: funcAddr ? this.getFuncLabel(collection, funcMd5, funcAddr) : 'Details',
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
                    segments.push({
                        label: this.getFileLabel(featMd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', featMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                segments.push({
                    label: 'Functions',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', featMd5, 'functions']),
                    icon: 'fa-solid fa-code'
                });
                if (params.get('md5') && params.get('address')) {
                    segments.push({
                        label: this.getFuncLabel(collection, featMd5, params.get('address')),
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
                if (sourceMd5 && sourceAddr) {
                    segments.push({
                        label: 'Files',
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                        icon: 'fa-solid fa-file-code'
                    });
                    segments.push({
                        label: this.getFileLabel(sourceMd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', sourceMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                    segments.push({
                        label: 'Functions',
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', sourceMd5, 'functions']),
                        icon: 'fa-solid fa-code'
                    });
                    segments.push({
                        label: this.getFuncLabel(collection, sourceMd5, sourceAddr),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['function', sourceMd5, sourceAddr]),
                        icon: 'fa-solid fa-code'
                    });
                    const targetColl = restful.coll_b || collection;
                    const targetMd5 = restful.md5_b;
                    const targetAddr = restful.addr_b;
                    let targetLabel = 'VS';
                    if (targetMd5 && targetAddr) {
                        targetLabel = `VS ${this.getFuncLabel(targetColl, targetMd5, targetAddr)}`;
                    }
                    segments.push({
                        label: targetLabel,
                        url: window.location.pathname,
                        icon: 'fa-solid fa-right-left'
                    });
                } else if (sourceMd5) {
                    segments.push({
                        label: 'Files',
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                        icon: 'fa-solid fa-file-code'
                    });
                    segments.push({
                        label: this.getFileLabel(sourceMd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', sourceMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                    const targetMd5 = restful.md5_b;
                    let targetLabel = 'VS';
                    if (targetMd5) {
                        targetLabel = `VS ${this.getFileLabel(targetMd5)}`;
                    }
                    segments.push({
                        label: targetLabel,
                        url: window.location.pathname,
                        icon: 'fa-solid fa-right-left'
                    });
                }
                break;
            case 'call_graph':
                segments.push({
                    label: 'Files',
                    url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['files']),
                    icon: 'fa-solid fa-file-code'
                });
                const cgMd5 = params.get('md5') || restful.md5;
                if (cgMd5) {
                    segments.push({
                        label: this.getFileLabel(cgMd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', cgMd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                    segments.push({
                        label: 'Functions',
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', cgMd5, 'functions']),
                        icon: 'fa-solid fa-code'
                    });
                }
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
                    segments.push({
                        label: this.getFileLabel(fmd5),
                        url: (window.Nav || window.parent.Nav).buildUIUrl(collection, ['file', fmd5]),
                        icon: 'fa-solid fa-file-code'
                    });
                }
                const bMd5 = restful.md5_b;
                let bLabel = 'VS';
                if (bMd5) {
                    bLabel = this.getFileLabel(bMd5) !== 'File' ? `VS ${this.getFileLabel(bMd5)}` : 'VS File';
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

        if (segments.length === 0) {
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
