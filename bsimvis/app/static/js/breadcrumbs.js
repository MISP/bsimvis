// bsimvis/app/static/js/breadcrumbs.js

window.Breadcrumbs = {
    generate: function(routingState, route) {
        const { viewKey, collection, params } = routingState;
        const restful = window.parseRestfulPath ? window.parseRestfulPath() : {};
        let segments = [];

        // Always start with the collection segment, if it's not the default view
        if (viewKey !== 'collections' && collection) {
            segments.push({
                label: collection,
                url: `/collections/${encodeURIComponent(collection)}/files`,
                icon: 'fa-solid fa-database'
            });
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
            case 'binary-similarity':
                segments.push({
                    label: route ? route.title : 'Similarities',
                    url: window.location.pathname + window.location.search,
                    icon: 'fa-solid fa-code-compare'
                });
                break;
            case 'clusters':
            case 'bin-clusters':
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
                    url: `/collections/${encodeURIComponent(collection)}/files`,
                    icon: 'fa-solid fa-file-code'
                });
                segments.push({
                    label: params.get('md5') ? (params.get('md5').substring(0, 12) + '…') : 'Details',
                    url: window.location.pathname,
                    icon: 'fa-solid fa-fingerprint'
                });
                break;
            case 'function':
                segments.push({
                    label: 'Files',
                    url: `/collections/${encodeURIComponent(collection)}/files`,
                    icon: 'fa-solid fa-file-code'
                });
                if (params.get('md5')) {
                    segments.push({
                        label: params.get('md5').substring(0, 12) + '…',
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(params.get('md5'))}`,
                        icon: 'fa-solid fa-fingerprint'
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
                    url: `/collections/${encodeURIComponent(collection)}/files`,
                    icon: 'fa-solid fa-file-code'
                });
                if (params.get('md5')) {
                    segments.push({
                        label: params.get('md5').substring(0, 12) + '…',
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(params.get('md5'))}`,
                        icon: 'fa-solid fa-fingerprint'
                    });
                }
                if (params.get('md5') && params.get('address')) {
                    segments.push({
                        label: `Func @${params.get('address')}`,
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(params.get('md5'))}/functions/${encodeURIComponent(params.get('address'))}`,
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
                    segments.push({
                        label: sourceMd5.substring(0, 12) + '…',
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(sourceMd5)}`,
                        icon: 'fa-solid fa-file-code'
                    });
                }
                if (sourceMd5 && sourceAddr) {
                    const funcLabel = window.currentFuncName && window.currentFuncId && window.currentFuncId.includes(sourceAddr) ? window.currentFuncName : `Func @${sourceAddr}`;
                    segments.push({
                        label: funcLabel,
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(sourceMd5)}/functions/${encodeURIComponent(sourceAddr)}`,
                        icon: 'fa-solid fa-code'
                    });
                }
                const targetColl = restful.coll_b || collection;
                const targetMd5 = restful.md5_b;
                const targetAddr = restful.addr_b;
                let targetLabel = 'VS';
                if (targetMd5 && targetAddr) {
                    targetLabel = `VS ${targetMd5.substring(0, 8)}@${targetAddr}`;
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
                    url: `/collections/${encodeURIComponent(collection)}/files`,
                    icon: 'fa-solid fa-file-code'
                });
                segments.push({
                    label: params.get('md5') ? (params.get('md5').substring(0, 12) + '…') : 'Details',
                    url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(params.get('md5'))}`,
                    icon: 'fa-solid fa-fingerprint'
                });
                segments.push({
                    label: 'Call Graph',
                    url: window.location.pathname,
                    icon: 'fa-solid fa-network-wired'
                });
                break;
            case 'feature':
                segments.push({
                    label: 'Global Features',
                    url: `/collections/${encodeURIComponent(collection)}/features`,
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
                    url: `/collections/${encodeURIComponent(collection)}/files`,
                    icon: 'fa-solid fa-file-code'
                });
                const fmd5 = restful.md5 || params.get('md5');
                if (fmd5) {
                    segments.push({
                        label: fmd5.substring(0, 12) + '…',
                        url: `/collections/${encodeURIComponent(collection)}/files/${encodeURIComponent(fmd5)}`,
                        icon: 'fa-solid fa-fingerprint'
                    });
                }
                const bMd5 = restful.md5_b;
                let bLabel = 'VS';
                if (bMd5) {
                    bLabel = `VS ${bMd5.substring(0, 12)}…`;
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

    render: function(segments) {
        const container = document.getElementById('breadcrumbs-container');
        if (!container) return;

        if (segments.length <= 1) {
            container.innerHTML = '';
            container.style.display = 'none';
            return;
        }
        container.style.display = 'block';

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
    }
};
