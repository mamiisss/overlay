-- Combined overlay runtime + Obsidian UI loader.
-- Runs overlay-runtime/client.lua, waits briefly for getgenv().OverlayBridge,
-- then mounts the bridge UI. This is the one-file local test path.

local EMBEDDED_RUNTIME_CLIENT = [==========[--[[
  Overlay Runtime Client v0.1

  Single-file Luau client for the overlay backend (docs/PROTOCOL.md, docs/CLIENT_GUIDE.md).
  Load this file in an executor that exposes a WebSocket API with text messages.

  Vertical slice scope:
    - connect / hello / auth / join room
    - apply room.snapshot and ordered room.delta messages
    - render remote entities through native-character or proxy-avatar adapters
    - stream the local character transform via state.move
    - interpolate remote transforms from room.state with a delay buffer
    - reconnect with backoff and resume from the last applied room version

  Renderer note:
    NativeCharacterOverlayRenderer anchors name/aura overlays to a same-server
    Roblox Character. ProxyAvatarRenderer is the fallback for users who are not
    present in this Roblox server; the current proxy body is a simple v0 part.

  Stop a running client with: getgenv().OverlayStop()
]]

local CONFIG = {
	url = "ws://127.0.0.1:8080/realtime",
	token = nil, -- nil -> "dev:<LocalPlayer.Name>" (dev tokens work while NODE_ENV ~= production)
	room_name = "Overlay Dev Room",
	client_name = "overlay-runtime",
	build = "0.1.0",
	move_hz = 10,
	move_keepalive_seconds = 10,
	interp_delay = 0.25, -- seconds of buffering for remote movement (server ticks at 125ms)
	request_timeout = 5,
	reconnect_max_delay = 15,
	follow_room_route = true,
	auto_join_default_room = false,
	max_native_highlights = 25,
	asset_cache_folder = "overlay-cache/assets",
	asset_max_bytes = 32 * 1024 * 1024,
	asset_require_hash_verification = false,
	asset_catalog_url = nil,
}

local HttpService = game:GetService("HttpService")
local Players = game:GetService("Players")
local RunService = game:GetService("RunService")
local TeleportService = game:GetService("TeleportService")

local LocalPlayer = Players.LocalPlayer
local GLOBAL = typeof(getgenv) == "function" and getgenv() or _G
if type(GLOBAL.OverlayAssetCatalogUrl) == "string" then
	CONFIG.asset_catalog_url = GLOBAL.OverlayAssetCatalogUrl
end

local function log(...)
	print("[overlay]", ...)
end

if type(GLOBAL.OverlayStop) == "function" then
	pcall(GLOBAL.OverlayStop)
end

-- ---------------------------------------------------------------------------
-- WebSocket adapter (executor APIs differ; add your executor here if needed)
-- ---------------------------------------------------------------------------

local function wsConnect(url)
	local candidates = {}

	local function globalValue(name)
		if type(GLOBAL) == "table" and GLOBAL[name] ~= nil then
			return GLOBAL[name]
		end
		return _G[name]
	end

	local function addCandidate(label, owner, method)
		if type(method) == "function" then
			table.insert(candidates, {
				label = label,
				connect = function(targetUrl)
					return method(owner, targetUrl)
				end,
			})
			table.insert(candidates, {
				label = label .. " direct",
				connect = function(targetUrl)
					return method(targetUrl)
				end,
			})
		end
	end

	for _, name in ipairs({ "WebSocket", "Websocket", "websocket" }) do
		local api = globalValue(name)
		if type(api) == "table" then
			addCandidate(name .. ".connect", api, api.connect)
			addCandidate(name .. ".Connect", api, api.Connect)
		end
	end

	local synApi = globalValue("syn")
	if type(synApi) == "table" and type(synApi.websocket) == "table" then
		addCandidate("syn.websocket.connect", synApi.websocket, synApi.websocket.connect)
		addCandidate("syn.websocket.Connect", synApi.websocket, synApi.websocket.Connect)
	end

	local function callSocketMethod(method, raw, ...)
		local ok, result = pcall(method, raw, ...)
		if ok then
			return result
		end
		return method(...)
	end

	local function normalizeSocket(raw)
		if raw == nil then
			return nil
		end
		local sendMethod = raw.Send or raw.send
		local closeMethod = raw.Close or raw.close
		local onMessage = raw.OnMessage or raw.MessageReceived or raw.Message
		local onClose = raw.OnClose or raw.Closed or raw.CloseEvent
		if type(sendMethod) ~= "function" or type(closeMethod) ~= "function" then
			return nil
		end
		if onMessage == nil or onClose == nil then
			return nil
		end
		if type(onMessage.Connect) ~= "function" or type(onClose.Connect) ~= "function" then
			return nil
		end
		return {
			raw = raw,
			OnMessage = onMessage,
			OnClose = onClose,
			Send = function(_, message)
				return callSocketMethod(sendMethod, raw, message)
			end,
			Close = function()
				return callSocketMethod(closeMethod, raw)
			end,
		}
	end

	for _, candidate in ipairs(candidates) do
		local ok, ws = pcall(candidate.connect, url)
		if ok then
			local normalized = normalizeSocket(ws)
			if normalized then
				return normalized
			end
		end
	end
	return nil
end

-- ---------------------------------------------------------------------------
-- Client state
-- ---------------------------------------------------------------------------

local Client = {
	running = true,
	ws = nil,
	connected = false,
	nextId = 0,
	pending = {}, -- request_id -> { callback, expiresAt }
	user = nil, -- { user_id, display_name }
	room = nil, -- { id, version }
	joining = nil, -- { roomId, resumeFrom }
	deferredSnapshot = nil,
	ownAvatarId = nil,
	avatarReadyRoomId = nil,
	entities = {}, -- entity_id -> { components, model, label, samples }
	connections = {},
	wsConnections = {},
	sessionId = 0,
	heartbeatSeconds = 15,
	lastServerContact = 0,
	teleporting = false,
	roomClosingId = nil,
	bridgeSubscribers = {},
	cachedRooms = {},
	assetManifests = {},
	assetCache = {},
	assetRequests = {},
	assetDescriptors = {},
	assetCatalog = {},
	assetCatalogById = {},
	previewHandle = nil,
}

local refreshRenderers

local entityFolder = Instance.new("Folder")
entityFolder.Name = "OverlayEntities"
entityFolder.Parent = workspace

-- ---------------------------------------------------------------------------
-- Protocol helpers
-- ---------------------------------------------------------------------------

local function trackConnection(connection)
	if connection then
		table.insert(Client.connections, connection)
	end
	return connection
end

local function disconnectTrackedConnections()
	for _, connection in ipairs(Client.connections) do
		pcall(function()
			connection:Disconnect()
		end)
	end
	Client.connections = {}
end

local function trackWsConnection(connection)
	if connection then
		table.insert(Client.wsConnections, connection)
	end
	return connection
end

local function disconnectWsConnections()
	for _, connection in ipairs(Client.wsConnections) do
		pcall(function()
			connection:Disconnect()
		end)
	end
	Client.wsConnections = {}
end

local function sameCFrameForMove(previous, current)
	if previous == nil then
		return false
	end
	if (current.Position - previous.Position).Magnitude >= 0.05 then
		return false
	end
	local delta = previous:ToObjectSpace(current)
	local rx, ry, rz = delta:ToOrientation()
	local maxAngle = math.max(math.abs(rx), math.abs(ry), math.abs(rz))
	return math.deg(maxAngle) < 1
end

local function nextMessageId()
	Client.nextId += 1
	return "msg_" .. Client.nextId
end

local function sendMessageWithId(id, messageType, data)
	if not Client.connected or Client.ws == nil then
		return false
	end
	local ok, encoded = pcall(HttpService.JSONEncode, HttpService, {
		t = messageType,
		id = id,
		v = 1,
		data = data or {},
	})
	if not ok then
		return false
	end
	local sent = pcall(function()
		Client.ws:Send(encoded)
	end)
	if not sent then
		Client.connected = false
		return false
	end
	return true
end

local function sendMessage(messageType, data)
	local id = nextMessageId()
	if not sendMessageWithId(id, messageType, data) then
		return nil
	end
	return id
end

local function awaitRequest(messageType, data, timeoutSeconds)
	local id = nextMessageId()
	local result
	Client.pending[id] = {
		callback = function(message)
			result = message
		end,
		expiresAt = os.clock() + (timeoutSeconds or CONFIG.request_timeout),
	}
	if not sendMessageWithId(id, messageType, data) then
		Client.pending[id] = nil
		return nil, "not connected"
	end

	local deadline = os.clock() + (timeoutSeconds or CONFIG.request_timeout)
	while result == nil and os.clock() < deadline and Client.running and Client.connected do
		task.wait(0.05)
	end
	Client.pending[id] = nil

	if result == nil then
		return nil, "timeout waiting for " .. messageType
	end
	if result.t == "error" then
		return nil, tostring(result.data and result.data.code) .. ": " .. tostring(result.data and result.data.message)
	end
	return result
end

local function currentRobloxCreateRoute()
	return {
		roblox_place_id = game.PlaceId,
		roblox_job_id = tostring(game.JobId or ""),
		roblox_universe_id = game.GameId,
	}
end

local function currentRobloxJoinContext()
	return {
		client_roblox_place_id = game.PlaceId,
		client_roblox_job_id = tostring(game.JobId or ""),
		client_roblox_universe_id = game.GameId,
	}
end

local function mergeTables(left, right)
	local merged = {}
	for key, value in pairs(left or {}) do
		merged[key] = value
	end
	for key, value in pairs(right or {}) do
		merged[key] = value
	end
	return merged
end

local function sameRobloxRoute(route)
	if type(route) ~= "table" or tonumber(route.place_id) == nil then
		return true
	end
	if tonumber(route.place_id) ~= game.PlaceId then
		return false
	end
	local jobId = type(route.job_id) == "string" and route.job_id or ""
	if jobId ~= "" and jobId ~= tostring(game.JobId or "") then
		return false
	end
	return true
end

local function tryTeleportToRoute(route, roomId)
	if not CONFIG.follow_room_route then
		return false, "room route following disabled"
	end
	if type(route) ~= "table" or tonumber(route.place_id) == nil then
		return false, "missing roblox route"
	end
	if sameRobloxRoute(route) then
		return false, "already in target Roblox server"
	end

	pcall(function()
		TeleportService:SetTeleportSetting("overlay_room_id", roomId)
	end)

	local placeId = tonumber(route.place_id)
	local jobId = type(route.job_id) == "string" and route.job_id or ""
	Client.teleporting = true
	log("teleporting to room server", "place", placeId, "job", jobId ~= "" and jobId or "<any>")
	local ok, err = pcall(function()
		if jobId ~= "" then
			TeleportService:TeleportToPlaceInstance(placeId, jobId, LocalPlayer)
		else
			TeleportService:Teleport(placeId, LocalPlayer)
		end
	end)
	if ok then
		return true, nil
	end

	Client.teleporting = false
	return false, tostring(err)
end

local function pendingRoomFromTeleportSetting()
	local ok, roomId = pcall(function()
		return TeleportService:GetTeleportSetting("overlay_room_id")
	end)
	if ok and type(roomId) == "string" and roomId ~= "" then
		pcall(function()
			TeleportService:SetTeleportSetting("overlay_room_id", nil)
		end)
		return roomId
	end
	return nil
end

local function trimString(value)
	if value == nil then
		return ""
	end
	return tostring(value):match("^%s*(.-)%s*$")
end

local function emitBridgeEvent(eventName, payload)
	for _, callback in ipairs(Client.bridgeSubscribers) do
		task.spawn(function()
			pcall(callback, eventName, payload or {})
		end)
	end
end

local function emitBridgeState(state)
	emitBridgeEvent("diagnostics.state", { state = state })
end

local function emitBridgeError(code, message)
	emitBridgeEvent("error", {
		code = code or "runtime.error",
		message = message or code or "Runtime error",
	})
end

-- ---------------------------------------------------------------------------
-- Asset cache (manifest/download/cache separated from render adapters)
-- ---------------------------------------------------------------------------

local AssetCache = {}

local function getGlobalFunction(name)
	if type(GLOBAL) == "table" and type(GLOBAL[name]) == "function" then
		return GLOBAL[name]
	end
	if type(_G) == "table" and type(_G[name]) == "function" then
		return _G[name]
	end
	return nil
end

local function firstCallable(...)
	local values = { ... }
	for _, value in ipairs(values) do
		if type(value) == "function" then
			return value
		end
	end
	return nil
end

local function requestHttp(url)
	local candidates = {
		getGlobalFunction("request"),
		getGlobalFunction("http_request"),
		typeof(syn) == "table" and syn.request or nil,
		typeof(http) == "table" and http.request or nil,
	}
	for _, requestFn in ipairs(candidates) do
		if type(requestFn) == "function" then
			local ok, response = pcall(requestFn, {
				Url = url,
				Method = "GET",
			})
			if ok and response then
				return response
			end
			ok, response = pcall(requestFn, url)
			if ok and response then
				return response
			end
		end
	end
	return nil
end

local function responseBody(response)
	if type(response) == "string" then
		return response
	end
	if type(response) ~= "table" then
		return nil
	end
	if response.Success == false then
		return nil
	end
	local status = tonumber(response.StatusCode or response.status_code or response.status)
	if status and status >= 400 then
		return nil
	end
	return response.Body or response.body or response.Data or response.data
end

local function ensureFolder(path)
	local makefolderFn = getGlobalFunction("makefolder")
	local isfolderFn = getGlobalFunction("isfolder")
	if not makefolderFn or not isfolderFn then
		return false
	end

	local current = ""
	for part in string.gmatch(path, "[^/]+") do
		current = current == "" and part or (current .. "/" .. part)
		if not isfolderFn(current) then
			local ok = pcall(makefolderFn, current)
			if not ok and not isfolderFn(current) then
				return false
			end
		end
	end
	return true
end

local function safeFileName(value)
	value = tostring(value or "asset")
	value = value:gsub("[^%w%._%-]", "_")
	return string.sub(value, 1, 96)
end

local function assetExtension(asset)
	local url = tostring(asset and asset.url or "")
	local path = string.match(url, "^[^%?]+") or url
	local ext = string.match(path, "%.([%w]+)$")
	if ext and #ext <= 8 then
		return "." .. string.lower(ext)
	end
	local assetType = string.lower(tostring(asset and asset.type or ""))
	if string.find(assetType, "texture", 1, true) or string.find(assetType, "image", 1, true) then
		return ".png"
	end
	if string.find(assetType, "mesh", 1, true) then
		return ".mesh"
	end
	return ".bin"
end

local function assetCachePath(asset)
	local key = safeFileName((asset.asset_id or "asset") .. "_" .. (asset.hash or "nohash"))
	return CONFIG.asset_cache_folder .. "/" .. key .. assetExtension(asset)
end

local function calculateSha256(body)
	local cryptTable = typeof(crypt) == "table" and crypt or nil
	local synCryptTable = typeof(syn) == "table" and typeof(syn.crypt) == "table" and syn.crypt or nil
	local hashFn = firstCallable(
		cryptTable and cryptTable.hash,
		synCryptTable and synCryptTable.hash,
		getGlobalFunction("sha256")
	)
	if not hashFn then
		return nil
	end
	local attempts = {
		function()
			return hashFn(body, "sha256")
		end,
		function()
			return hashFn("sha256", body)
		end,
		function()
			return hashFn(body)
		end,
	}
	for _, attempt in ipairs(attempts) do
		local ok, value = pcall(attempt)
		if ok and type(value) == "string" and value ~= "" then
			return string.lower(value)
		end
	end
	return nil
end

local function verifyAssetBody(asset, body)
	local expected = string.lower(tostring(asset.hash or ""))
	if string.sub(expected, 1, 7) == "sha256:" then
		local actual = calculateSha256(body)
		if actual then
			return actual == string.sub(expected, 8), actual == string.sub(expected, 8) and "sha256" or "sha256_mismatch"
		end
		return not CONFIG.asset_require_hash_verification, "sha256_unavailable"
	end
	if string.sub(expected, 1, 4) == "dev:" or string.sub(expected, 1, 7) == "unsafe:" then
		return not CONFIG.asset_require_hash_verification, "dev_hash"
	end
	return not CONFIG.asset_require_hash_verification, "unsupported_hash"
end

function AssetCache.localUrl(assetOrId)
	local assetId = type(assetOrId) == "table" and assetOrId.asset_id or assetOrId
	if not assetId then
		return nil
	end
	local cached = Client.assetCache[assetId]
	if cached and cached.localUrl then
		return cached.localUrl
	end

	local asset = type(assetOrId) == "table" and assetOrId or Client.assetManifests[assetId]
	if not asset then
		return nil
	end
	local path = assetCachePath(asset)
	local isfileFn = getGlobalFunction("isfile")
	local getcustomassetFn = getGlobalFunction("getcustomasset")
	if not isfileFn or not getcustomassetFn or not isfileFn(path) then
		return nil
	end
	local ok, localUrl = pcall(getcustomassetFn, path)
	if not ok or not localUrl then
		return nil
	end
	Client.assetCache[assetId] = {
		status = "ready",
		path = path,
		localUrl = localUrl,
		verified = cached and cached.verified or false,
	}
	return localUrl
end

function AssetCache.localPath(assetOrId)
	local assetId = type(assetOrId) == "table" and assetOrId.asset_id or assetOrId
	if not assetId then
		return nil
	end

	local cached = Client.assetCache[assetId]
	if cached and cached.path then
		return cached.path
	end

	local asset = type(assetOrId) == "table" and assetOrId or Client.assetManifests[assetId]
	if not asset then
		return nil
	end
	local path = assetCachePath(asset)
	local isfileFn = getGlobalFunction("isfile")
	if isfileFn and isfileFn(path) then
		return path
	end
	return nil
end

function AssetCache.body(assetOrId)
	local path = AssetCache.localPath(assetOrId)
	local readfileFn = getGlobalFunction("readfile")
	if not path or not readfileFn then
		return nil
	end
	local ok, body = pcall(readfileFn, path)
	if ok and type(body) == "string" then
		return body
	end
	return nil
end

function AssetCache.requestManifest(assetId)
	assetId = tostring(assetId or "")
	if assetId == "" or Client.assetManifests[assetId] or Client.assetRequests["manifest:" .. assetId] then
		return
	end
	Client.assetRequests["manifest:" .. assetId] = true
	task.spawn(function()
		local response = nil
		if Client.connected then
			response = awaitRequest("asset.get", { asset_id = assetId }, CONFIG.request_timeout)
		end
		Client.assetRequests["manifest:" .. assetId] = nil
		local asset = response and response.data and response.data.asset
		if asset then
			AssetCache.registerAssets({ asset })
		end
	end)
end

function AssetCache.ensure(asset)
	if type(asset) ~= "table" or type(asset.asset_id) ~= "string" or type(asset.url) ~= "string" then
		return false
	end
	Client.assetManifests[asset.asset_id] = asset
	if AssetCache.localUrl(asset) then
		return true
	end
	if Client.assetRequests["download:" .. asset.asset_id] then
		return false
	end

	local writefileFn = getGlobalFunction("writefile")
	if not writefileFn or not getGlobalFunction("isfile") or not getGlobalFunction("getcustomasset") then
		Client.assetCache[asset.asset_id] = { status = "missing_filesystem" }
		return false
	end
	if asset.size and tonumber(asset.size) and tonumber(asset.size) > CONFIG.asset_max_bytes then
		Client.assetCache[asset.asset_id] = { status = "too_large" }
		return false
	end

	Client.assetRequests["download:" .. asset.asset_id] = true
	task.spawn(function()
		local body = responseBody(requestHttp(asset.url))
		if type(body) ~= "string" then
			Client.assetCache[asset.asset_id] = { status = "download_failed" }
			Client.assetRequests["download:" .. asset.asset_id] = nil
			emitBridgeEvent("asset.cached", { asset_id = asset.asset_id, status = "download_failed" })
			return
		end
		if #body > CONFIG.asset_max_bytes then
			Client.assetCache[asset.asset_id] = { status = "too_large" }
			Client.assetRequests["download:" .. asset.asset_id] = nil
			emitBridgeEvent("asset.cached", { asset_id = asset.asset_id, status = "too_large" })
			return
		end

		local verified, verification = verifyAssetBody(asset, body)
		if not verified and CONFIG.asset_require_hash_verification then
			Client.assetCache[asset.asset_id] = { status = "verify_failed", verification = verification }
			Client.assetRequests["download:" .. asset.asset_id] = nil
			emitBridgeEvent("asset.cached", { asset_id = asset.asset_id, status = "verify_failed", verification = verification })
			return
		end

		if not ensureFolder(CONFIG.asset_cache_folder) then
			Client.assetCache[asset.asset_id] = { status = "cache_folder_failed" }
			Client.assetRequests["download:" .. asset.asset_id] = nil
			emitBridgeEvent("asset.cached", { asset_id = asset.asset_id, status = "cache_folder_failed" })
			return
		end

		local path = assetCachePath(asset)
		local ok = pcall(writefileFn, path, body)
		if not ok then
			Client.assetCache[asset.asset_id] = { status = "write_failed" }
			Client.assetRequests["download:" .. asset.asset_id] = nil
			emitBridgeEvent("asset.cached", { asset_id = asset.asset_id, status = "write_failed" })
			return
		end

		local localUrl = AssetCache.localUrl(asset)
		Client.assetCache[asset.asset_id] = {
			status = localUrl and "ready" or "custom_asset_failed",
			path = path,
			localUrl = localUrl,
			verified = verified,
			verification = verification,
		}
		Client.assetRequests["download:" .. asset.asset_id] = nil
		emitBridgeEvent("asset.cached", {
			asset_id = asset.asset_id,
			status = Client.assetCache[asset.asset_id].status,
			verified = verified,
			verification = verification,
		})
		if refreshRenderers then
			refreshRenderers()
		end
	end)
	return false
end

function AssetCache.registerAssets(assets)
	for _, asset in ipairs(assets or {}) do
		if type(asset) == "table" and type(asset.asset_id) == "string" then
			Client.assetManifests[asset.asset_id] = asset
			AssetCache.ensure(asset)
			for _, dependencyId in ipairs(asset.dependencies or {}) do
				AssetCache.requestManifest(dependencyId)
			end
		end
	end
end

local function registerCatalogAssets(assets)
	Client.assetCatalog = {}
	Client.assetCatalogById = {}
	for _, asset in ipairs(assets or {}) do
		if type(asset) == "table" and type(asset.asset_id) == "string" then
			table.insert(Client.assetCatalog, asset)
			Client.assetCatalogById[asset.asset_id] = asset
		end
	end
	emitBridgeEvent("asset.catalog", {
		assets = Client.assetCatalog,
		total = #Client.assetCatalog,
	})
end

local function loadAssetCatalog(url)
	url = trimString(url)
	if url == "" then
		emitBridgeError("asset.catalog_url_required", "Catalog URL is required")
		return
	end

	task.spawn(function()
		local body = responseBody(requestHttp(url))
		if type(body) ~= "string" then
			emitBridgeError("asset.catalog_failed", "Could not fetch asset catalog")
			return
		end

		local ok, catalog = pcall(HttpService.JSONDecode, HttpService, body)
		if not ok or type(catalog) ~= "table" or type(catalog.assets) ~= "table" then
			emitBridgeError("asset.catalog_invalid", "Invalid asset catalog")
			return
		end

		local function catalogHttpUrl(value)
			value = string.lower(trimString(value))
			return string.sub(value, 1, 7) == "http://" or string.sub(value, 1, 8) == "https://"
		end
		local baseUrl = trimString(catalog.base_url)
		if baseUrl == "" then
			baseUrl = string.match(url, "^(.*)/[^/]*$") or url
		end
		for _, asset in ipairs(catalog.assets) do
			if type(asset) == "table" and type(asset.url) == "string" and not catalogHttpUrl(asset.url) then
				asset.url = baseUrl:gsub("/+$", "") .. "/" .. asset.url:gsub("^/+", "")
			end
		end

		registerCatalogAssets(catalog.assets)
		emitBridgeState("asset catalog loaded: " .. tostring(#Client.assetCatalog))
	end)
end

local function collectAssetIdsFromValue(value, out, depth)
	if type(value) ~= "table" or depth > 8 then
		return
	end
	if type(value.asset_id) == "string" then
		out[value.asset_id] = true
	end
	for _, child in pairs(value) do
		if type(child) == "table" then
			collectAssetIdsFromValue(child, out, depth + 1)
		end
	end
end

local function prefetchAssetRefs(components)
	local ids = {}
	collectAssetIdsFromValue(components, ids, 0)
	for assetId in pairs(ids) do
		local asset = Client.assetManifests[assetId]
		if asset then
			AssetCache.ensure(asset)
		else
			AssetCache.requestManifest(assetId)
		end
	end
end

local function isOverlayDescriptorAsset(asset)
	if type(asset) ~= "table" then
		return false
	end
	local format = string.lower(tostring(asset.format or ""))
	local typeName = string.lower(tostring(asset.type or asset.kind or ""))
	local url = string.lower(tostring(asset.url or ""))
	return format == "overlay_json_v1"
		or string.find(typeName, "morph_bundle", 1, true) ~= nil
		or string.find(url, "%.json", 1, false) ~= nil
end

local function descriptorForAsset(assetId)
	assetId = trimString(assetId)
	if assetId == "" then
		return nil
	end
	if Client.assetDescriptors[assetId] then
		return Client.assetDescriptors[assetId]
	end

	local asset = Client.assetManifests[assetId]
	if not asset then
		AssetCache.requestManifest(assetId)
		return nil
	end
	if not isOverlayDescriptorAsset(asset) then
		return nil
	end

	local body = AssetCache.body(asset)
	if type(body) ~= "string" then
		AssetCache.ensure(asset)
		return nil
	end

	local ok, descriptor = pcall(HttpService.JSONDecode, HttpService, body)
	if not ok or type(descriptor) ~= "table" or descriptor.format ~= "overlay_json_v1" or type(descriptor.nodes) ~= "table" then
		Client.assetDescriptors[assetId] = false
		emitBridgeError("asset.descriptor_invalid", "Invalid overlay descriptor: " .. tostring(assetId))
		return nil
	end

	Client.assetDescriptors[assetId] = descriptor
	emitBridgeEvent("asset.descriptor.ready", {
		asset_id = assetId,
		name = descriptor.name,
		nodes = #descriptor.nodes,
	})
	return descriptor
end

-- ---------------------------------------------------------------------------
-- Renderer adapters
-- ---------------------------------------------------------------------------

local function colorFromId(entityId)
	local hash = 0
	for i = 1, #entityId do
		hash = (hash * 31 + string.byte(entityId, i)) % 16777216
	end
	return Color3.fromHSV((hash % 360) / 360, 0.6, 0.9)
end

local function colorToArray(color)
	return { color.R, color.G, color.B }
end

local function colorFromComponents(entityId, components)
	local avatar = components and components.avatar
	local custom = avatar and avatar.overlay_color
	if type(custom) == "table" then
		local r = tonumber(custom[1])
		local g = tonumber(custom[2])
		local b = tonumber(custom[3])
		if r and g and b then
			return Color3.new(math.clamp(r, 0, 1), math.clamp(g, 0, 1), math.clamp(b, 0, 1))
		end
	end
	if type(avatar) == "table" and tonumber(avatar.appearance_user_id) then
		return colorFromId(tostring(avatar.appearance_user_id))
	end
	return colorFromId(entityId)
end

local EFFECT_PRESETS = {
	none = { color = Color3.fromRGB(170, 170, 170) },
	violet = { color = Color3.fromRGB(155, 105, 255) },
	cyan = { color = Color3.fromRGB(80, 220, 255) },
	emerald = { color = Color3.fromRGB(80, 255, 170) },
	gold = { color = Color3.fromRGB(255, 205, 80) },
	rose = { color = Color3.fromRGB(255, 95, 150) },
}

local function colorFromEffect(effect, fallback)
	if type(effect) == "table" and type(effect.color) == "table" then
		local r = tonumber(effect.color[1])
		local g = tonumber(effect.color[2])
		local b = tonumber(effect.color[3])
		if r and g and b then
			return Color3.new(math.clamp(r, 0, 1), math.clamp(g, 0, 1), math.clamp(b, 0, 1))
		end
	end

	local presetName = "violet"
	if type(effect) == "table" and type(effect.preset) == "string" then
		presetName = string.lower(effect.preset)
	end
	local preset = EFFECT_PRESETS[presetName]
	return (preset and preset.color) or fallback
end

local function effectSettings(entityId, components)
	local effect = type(components) == "table" and components.effect or nil
	local fallbackColor = colorFromComponents(entityId, components)
	local intensity = 35
	local preset = "violet"
	local trail = false
	if type(effect) == "table" then
		intensity = tonumber(effect.intensity) or tonumber(effect.density) or intensity
		if type(effect.preset) == "string" then
			preset = string.lower(effect.preset)
		end
		trail = effect.trail == true or effect.trail == "on" or effect.trail == "true"
	end
	intensity = math.clamp(intensity, 0, 100)
	if preset == "none" then
		intensity = 0
	end
	return {
		enabled = intensity > 0,
		intensity = intensity,
		alpha = intensity / 100,
		color = colorFromEffect(effect, fallbackColor),
		trail = trail,
	}
end

local function transformToCFrame(transform)
	local p = transform.position
	local r = transform.rotation
	local position = Vector3.new(
		tonumber(p and p[1]) or 0,
		tonumber(p and p[2]) or 5,
		tonumber(p and p[3]) or 0
	)
	local rx = math.rad(tonumber(r and r[1]) or 0)
	local ry = math.rad(tonumber(r and r[2]) or 0)
	local rz = math.rad(tonumber(r and r[3]) or 0)
	return CFrame.new(position) * CFrame.fromOrientation(rx, ry, rz)
end

local function displayNameFor(entityId, components)
	local avatar = components and components.avatar
	if type(avatar) == "table" and type(avatar.display_name) == "string" then
		return avatar.display_name
	end
	return entityId
end

local function robloxUserIdFor(components)
	local avatar = components and components.avatar
	if type(avatar) ~= "table" then
		return nil
	end
	local id = tonumber(avatar.roblox_user_id)
	if id == nil then
		return nil
	end
	return id
end

local function findPlayerByRobloxUserId(robloxUserId)
	if robloxUserId == nil then
		return nil
	end
	for _, player in ipairs(Players:GetPlayers()) do
		if player.UserId == robloxUserId then
			return player
		end
	end
	return nil
end

local function isOwnEntity(entityId, components)
	if entityId == Client.ownAvatarId then
		return true
	end
	return robloxUserIdFor(components) == LocalPlayer.UserId
end

local function createBillboard(parent, entityId, components, offset)
	local color = colorFromComponents(entityId, components)
	local billboard = Instance.new("BillboardGui")
	billboard.Name = "OverlayName"
	billboard.Size = UDim2.fromOffset(180, 30)
	billboard.StudsOffset = offset or Vector3.new(0, 2.6, 0)
	billboard.AlwaysOnTop = true
	billboard.Parent = parent

	local label = Instance.new("TextLabel")
	label.Name = "Label"
	label.Size = UDim2.fromScale(1, 1)
	label.BackgroundTransparency = 1
	label.TextScaled = true
	label.Font = Enum.Font.GothamBold
	label.TextColor3 = color
	label.TextStrokeColor3 = Color3.new(0, 0, 0)
	label.TextStrokeTransparency = 0.25
	label.Text = displayNameFor(entityId, components)
	label.Parent = billboard

	return billboard, label
end

local function destroyHandle(handle)
	if handle == nil then
		return
	end
	if handle.connections then
		for _, connection in ipairs(handle.connections) do
			pcall(function()
				connection:Disconnect()
			end)
		end
	end
	for _, key in ipairs({ "billboard", "highlight", "auraParticle", "auraAttachment", "trail", "trailAttachment0", "trailAttachment1", "meshVisual", "textureDecal", "descriptorMorphRoot", "appearanceModel", "model", "folder" }) do
		local instance = handle[key]
		if instance then
			pcall(function()
				instance:Destroy()
			end)
		end
	end
end

local function activeOverlayHighlightCount()
	local count = 0
	for _, entity in pairs(Client.entities) do
		if entity.handle and entity.handle.highlight then
			count += 1
		end
	end
	return count
end

local function pushSample(entity, transform)
	entity.samples = entity.samples or {}
	table.insert(entity.samples, { at = os.clock(), cf = transformToCFrame(transform) })
	while #entity.samples > 8 do
		table.remove(entity.samples, 1)
	end
end

local function assetIdFromComponent(component)
	if type(component) == "table" then
		return component.asset_id or component.mesh_asset_id or component.texture_asset_id
	end
	if type(component) == "string" then
		return component
	end
	return nil
end

local function assetLooksLikeKind(asset, kind)
	if type(asset) ~= "table" then
		return false
	end
	local typeName = string.lower(tostring(asset.type or ""))
	local url = string.lower(tostring(asset.url or ""))
	if string.find(typeName, kind, 1, true) then
		return true
	end
	if kind == "mesh" then
		return string.find(url, "%.mesh", 1, false) ~= nil
	end
	if kind == "texture" then
		return string.find(url, "%.png", 1, false) ~= nil
			or string.find(url, "%.jpg", 1, false) ~= nil
			or string.find(url, "%.jpeg", 1, false) ~= nil
			or string.find(url, "%.webp", 1, false) ~= nil
	end
	return false
end

local function localVisualAssetUrl(component, kind)
	local assetId = assetIdFromComponent(component)
	if not assetId then
		return nil
	end
	local asset = Client.assetManifests[assetId]
	if asset and not assetLooksLikeKind(asset, kind) then
		return nil
	end
	return AssetCache.localUrl(assetId)
end

local function applyProxyAssetVisuals(handle, parentPart, components)
	if handle == nil or parentPart == nil or type(components) ~= "table" then
		return
	end

	local meshUrl = localVisualAssetUrl(components.mesh, "mesh")
		or localVisualAssetUrl(components.morph, "mesh")
		or localVisualAssetUrl(components.avatar, "mesh")
	local textureUrl = localVisualAssetUrl(components.texture, "texture")
		or localVisualAssetUrl(components.morph, "texture")
		or localVisualAssetUrl(components.avatar, "texture")
	if meshUrl then
		if handle.meshVisual == nil or handle.meshVisual.Parent ~= parentPart then
			if handle.meshVisual then
				handle.meshVisual:Destroy()
			end
			local mesh = Instance.new("SpecialMesh")
			mesh.Name = "OverlayAssetMesh"
			mesh.MeshType = Enum.MeshType.FileMesh
			mesh.Parent = parentPart
			handle.meshVisual = mesh
		end
		handle.meshVisual.MeshId = meshUrl
		handle.meshVisual.TextureId = textureUrl or ""
		if handle.textureDecal then
			handle.textureDecal:Destroy()
			handle.textureDecal = nil
		end
	elseif handle.meshVisual then
		handle.meshVisual:Destroy()
		handle.meshVisual = nil
	end

	if textureUrl and not meshUrl then
		if handle.textureDecal == nil or handle.textureDecal.Parent ~= parentPart then
			if handle.textureDecal then
				handle.textureDecal:Destroy()
			end
			local decal = Instance.new("Decal")
			decal.Name = "OverlayAssetTexture"
			decal.Face = Enum.NormalId.Front
			decal.Parent = parentPart
			handle.textureDecal = decal
		end
		handle.textureDecal.Texture = textureUrl
	elseif handle.textureDecal then
		handle.textureDecal:Destroy()
		handle.textureDecal = nil
	end
end

local function descriptorVector3(value, fallback)
	if type(value) ~= "table" then
		return fallback or Vector3.new()
	end
	return Vector3.new(tonumber(value[1]) or 0, tonumber(value[2]) or 0, tonumber(value[3]) or 0)
end

local function descriptorVector2(value, fallback)
	if type(value) ~= "table" then
		return fallback or Vector2.new()
	end
	return Vector2.new(tonumber(value[1]) or 0, tonumber(value[2]) or 0)
end

local function descriptorColor(value, fallback)
	if type(value) ~= "table" then
		return fallback or Color3.new(1, 1, 1)
	end
	return Color3.new(
		math.clamp(tonumber(value[1]) or 1, 0, 1),
		math.clamp(tonumber(value[2]) or 1, 0, 1),
		math.clamp(tonumber(value[3]) or 1, 0, 1)
	)
end

local function descriptorCFrame(value)
	if type(value) ~= "table" then
		return CFrame.new()
	end
	local position = value.position or {}
	local rotation = value.rotation or {}
	return CFrame.new(
		tonumber(position[1]) or 0,
		tonumber(position[2]) or 0,
		tonumber(position[3]) or 0,
		tonumber(rotation[1]) or 1,
		tonumber(rotation[2]) or 0,
		tonumber(rotation[3]) or 0,
		tonumber(rotation[4]) or 0,
		tonumber(rotation[5]) or 1,
		tonumber(rotation[6]) or 0,
		tonumber(rotation[7]) or 0,
		tonumber(rotation[8]) or 0,
		tonumber(rotation[9]) or 1
	)
end

local function descriptorNumberRange(value, fallbackMin, fallbackMax)
	if type(value) == "table" then
		local minValue = tonumber(value[1]) or fallbackMin or 0
		local maxValue = tonumber(value[2]) or minValue
		return NumberRange.new(minValue, maxValue)
	end
	local numeric = tonumber(value)
	if numeric then
		return NumberRange.new(numeric)
	end
	return NumberRange.new(fallbackMin or 0, fallbackMax or fallbackMin or 0)
end

local function parseDescriptorNumbers(value)
	local numbers = {}
	for part in string.gmatch(tostring(value or ""), "%S+") do
		local numeric = tonumber(part)
		if numeric then
			table.insert(numbers, numeric)
		end
	end
	return numbers
end

local function descriptorNumberSequence(value, fallback)
	if type(value) ~= "string" or value == "" then
		return NumberSequence.new(fallback or 0)
	end
	local numbers = parseDescriptorNumbers(value)
	local maxValue = fallback or 0
	for index = 2, #numbers, 3 do
		maxValue = math.max(maxValue, numbers[index] or maxValue)
	end
	return NumberSequence.new(maxValue)
end

local function descriptorColorSequence(value, fallback)
	if type(value) ~= "string" or value == "" then
		return ColorSequence.new(fallback or Color3.new(1, 1, 1))
	end
	local numbers = parseDescriptorNumbers(value)
	if #numbers >= 4 then
		return ColorSequence.new(Color3.new(
			math.clamp(numbers[2] or 1, 0, 1),
			math.clamp(numbers[3] or 1, 0, 1),
			math.clamp(numbers[4] or 1, 0, 1)
		))
	end
	return ColorSequence.new(fallback or Color3.new(1, 1, 1))
end

local function descriptorContent(value)
	if type(value) ~= "string" then
		return nil
	end
	if value == "" or value == "null" then
		return nil
	end
	return value
end

local function safeSet(instance, property, value)
	if value == nil then
		return
	end
	pcall(function()
		instance[property] = value
	end)
end

local function safeGet(instance, property, fallback)
	local ok, value = pcall(function()
		return instance[property]
	end)
	if ok and value ~= nil then
		return value
	end
	return fallback
end

local function safeSetAttribute(instance, name, value)
	if instance == nil or type(name) ~= "string" or name == "" then
		return
	end
	if type(value) == "string" or type(value) == "number" or type(value) == "boolean" then
		pcall(function()
			instance:SetAttribute(name, value)
		end)
	elseif type(value) == "table" and #value == 3 then
		pcall(function()
			instance:SetAttribute(name, Vector3.new(tonumber(value[1]) or 0, tonumber(value[2]) or 0, tonumber(value[3]) or 0))
		end)
	end
end

local function applyDescriptorAttributes(instance, attributes)
	if type(attributes) ~= "table" then
		return
	end
	for key, value in pairs(attributes) do
		safeSetAttribute(instance, tostring(key), value)
	end
end

local function applyBasePartDescriptor(part, properties, anchorPart)
	safeSet(part, "Size", descriptorVector3(properties.size or properties.Size, Vector3.new(1, 1, 1)))
	safeSet(part, "Color", descriptorColor(properties.Color3uint8 or properties.Color, Color3.new(1, 1, 1)))
	safeSet(part, "Transparency", math.clamp(tonumber(properties.Transparency) or 0, 0, 1))
	safeSet(part, "Reflectance", tonumber(properties.Reflectance) or 0)
	safeSet(part, "CanCollide", false)
	safeSet(part, "CanTouch", false)
	safeSet(part, "CanQuery", false)
	safeSet(part, "Massless", true)
	safeSet(part, "Anchored", false)
	safeSet(part, "TopSurface", Enum.SurfaceType.Smooth)
	safeSet(part, "BottomSurface", Enum.SurfaceType.Smooth)
	if anchorPart then
		part.CFrame = anchorPart.CFrame * descriptorCFrame(properties.LocalCFrame)
	end
end

local function createDescriptorInstance(node)
	local className = node.class
	if className == "Folder" then
		return Instance.new("Folder")
	elseif className == "Model" then
		return Instance.new("Model")
	elseif className == "Part" then
		return Instance.new("Part")
	elseif className == "WedgePart" or className == "CornerWedgePart" or className == "TrussPart" or className == "UnionOperation" then
		local ok, instance = pcall(Instance.new, className)
		return ok and instance or Instance.new("Part")
	elseif className == "MeshPart" then
		local instance = Instance.new("Part")
		safeSetAttribute(instance, "OverlaySourceClass", "MeshPart")
		return instance
	elseif className == "SpecialMesh" then
		return Instance.new("SpecialMesh")
	elseif className == "ParticleEmitter" then
		return Instance.new("ParticleEmitter")
	elseif className == "Attachment" then
		return Instance.new("Attachment")
	elseif className == "Bone" then
		local ok, instance = pcall(Instance.new, "Bone")
		return ok and instance or nil
	elseif className == "Highlight" then
		return Instance.new("Highlight")
	elseif className == "Decal" then
		return Instance.new("Decal")
	elseif className == "Texture" then
		return Instance.new("Texture")
	elseif className == "PointLight" then
		return Instance.new("PointLight")
	elseif className == "SpotLight" then
		return Instance.new("SpotLight")
	elseif className == "SurfaceLight" then
		return Instance.new("SurfaceLight")
	elseif className == "SurfaceAppearance" or className == "WrapLayer" or className == "WrapTarget" then
		local ok, instance = pcall(Instance.new, className)
		return ok and instance or nil
	elseif className == "Weld" or className == "Motor6D" or className == "WeldConstraint" then
		local ok, instance = pcall(Instance.new, className)
		return ok and instance or nil
	end
	return nil
end

local function applyAttachmentLikeDescriptor(instance, properties)
	if properties.CFrame then
		safeSet(instance, "CFrame", descriptorCFrame(properties.CFrame))
	else
		safeSet(instance, "Position", descriptorVector3(properties.Position, Vector3.new()))
		safeSet(instance, "Orientation", descriptorVector3(properties.Orientation, Vector3.new()))
	end
	safeSet(instance, "Axis", descriptorVector3(properties.Axis, safeGet(instance, "Axis", Vector3.new(1, 0, 0))))
	safeSet(instance, "SecondaryAxis", descriptorVector3(properties.SecondaryAxis, safeGet(instance, "SecondaryAxis", Vector3.new(0, 1, 0))))
end

local function applyDescriptorInstanceProperties(instance, node, anchorPart)
	local properties = type(node.properties) == "table" and node.properties or {}
	safeSet(instance, "Name", tostring(node.name or properties.Name or node.class or "OverlayNode"))
	applyDescriptorAttributes(instance, node.attributes)

	if instance:IsA("BasePart") then
		applyBasePartDescriptor(instance, properties, anchorPart)
		if node.class == "MeshPart" then
			local meshId = descriptorContent(properties.MeshId)
			if meshId then
				local mesh = instance:FindFirstChild("OverlayMeshPartFallback")
				if mesh == nil or not mesh:IsA("SpecialMesh") then
					mesh = Instance.new("SpecialMesh")
					mesh.Name = "OverlayMeshPartFallback"
					mesh.Parent = instance
				end
				safeSet(mesh, "MeshType", Enum.MeshType.FileMesh)
				safeSet(mesh, "MeshId", meshId)
				safeSet(mesh, "TextureId", descriptorContent(properties.TextureID or properties.TextureId))
				safeSet(mesh, "Scale", descriptorVector3(properties.size or properties.Size, Vector3.new(1, 1, 1)))
				safeSet(mesh, "Offset", Vector3.new())
			elseif instance:IsA("MeshPart") then
				safeSet(instance, "MeshId", descriptorContent(properties.MeshId))
				safeSet(instance, "TextureID", descriptorContent(properties.TextureID or properties.TextureId))
			end
		end
	elseif instance:IsA("SpecialMesh") then
		safeSet(instance, "MeshId", descriptorContent(properties.MeshId))
		safeSet(instance, "TextureId", descriptorContent(properties.TextureId))
		safeSet(instance, "Scale", descriptorVector3(properties.Scale, Vector3.new(1, 1, 1)))
		safeSet(instance, "Offset", descriptorVector3(properties.Offset, Vector3.new()))
		safeSet(instance, "VertexColor", descriptorVector3(properties.VertexColor, Vector3.new(1, 1, 1)))
		if descriptorContent(properties.MeshId) then
			safeSet(instance, "MeshType", Enum.MeshType.FileMesh)
		end
	elseif instance:IsA("ParticleEmitter") then
		safeSet(instance, "Texture", descriptorContent(properties.Texture))
		safeSet(instance, "Enabled", properties.Enabled ~= false)
		safeSet(instance, "Rate", tonumber(properties.Rate) or 8)
		safeSet(instance, "Lifetime", descriptorNumberRange(properties.Lifetime, 0.5, 1))
		safeSet(instance, "Speed", descriptorNumberRange(properties.Speed, 0, 1))
		safeSet(instance, "SpreadAngle", descriptorVector2(properties.SpreadAngle, Vector2.new(0, 0)))
		safeSet(instance, "Color", descriptorColorSequence(properties.Color, Color3.new(1, 1, 1)))
		safeSet(instance, "Size", descriptorNumberSequence(properties.Size, 0.2))
		safeSet(instance, "Transparency", descriptorNumberSequence(properties.Transparency, 0))
		safeSet(instance, "LightEmission", tonumber(properties.LightEmission) or 0)
		safeSet(instance, "LightInfluence", tonumber(properties.LightInfluence) or 0)
		safeSet(instance, "Acceleration", descriptorVector3(properties.Acceleration, Vector3.new()))
		safeSet(instance, "Drag", tonumber(properties.Drag) or 0)
		safeSet(instance, "LockedToPart", properties.LockedToPart == true)
		safeSet(instance, "TimeScale", tonumber(properties.TimeScale) or 1)
		safeSet(instance, "ZOffset", tonumber(properties.ZOffset) or 0)
		safeSet(instance, "Rotation", descriptorNumberRange(properties.Rotation, 0, 0))
		safeSet(instance, "RotSpeed", descriptorNumberRange(properties.RotSpeed, 0, 0))
		safeSet(instance, "VelocityInheritance", tonumber(properties.VelocityInheritance) or 0)
	elseif instance:IsA("Attachment") then
		applyAttachmentLikeDescriptor(instance, properties)
	elseif instance:IsA("Bone") then
		applyAttachmentLikeDescriptor(instance, properties)
		safeSet(instance, "Transform", descriptorCFrame(properties.Transform))
		safeSet(instance, "Visible", properties.Visible == true)
	elseif instance:IsA("Highlight") then
		safeSet(instance, "Enabled", properties.Enabled ~= false)
		safeSet(instance, "FillColor", descriptorColor(properties.FillColor, Color3.new(1, 1, 1)))
		safeSet(instance, "OutlineColor", descriptorColor(properties.OutlineColor, Color3.new(1, 1, 1)))
		safeSet(instance, "FillTransparency", math.clamp(tonumber(properties.FillTransparency) or 0.8, 0, 1))
		safeSet(instance, "OutlineTransparency", math.clamp(tonumber(properties.OutlineTransparency) or 0, 0, 1))
	elseif instance:IsA("Decal") or instance:IsA("Texture") then
		safeSet(instance, "Texture", descriptorContent(properties.Texture))
		safeSet(instance, "Transparency", math.clamp(tonumber(properties.Transparency) or 0, 0, 1))
		safeSet(instance, "Color3", descriptorColor(properties.Color3, Color3.new(1, 1, 1)))
	elseif instance:IsA("Light") then
		safeSet(instance, "Enabled", properties.Enabled ~= false)
		safeSet(instance, "Color", descriptorColor(properties.Color, Color3.new(1, 1, 1)))
		safeSet(instance, "Brightness", tonumber(properties.Brightness) or 1)
		safeSet(instance, "Range", tonumber(properties.Range) or 8)
		safeSet(instance, "Shadows", properties.Shadows == true)
	elseif instance:IsA("SurfaceAppearance") then
		safeSet(instance, "ColorMap", descriptorContent(properties.ColorMap))
		safeSet(instance, "MetalnessMap", descriptorContent(properties.MetalnessMap))
		safeSet(instance, "NormalMap", descriptorContent(properties.NormalMap))
		safeSet(instance, "RoughnessMap", descriptorContent(properties.RoughnessMap))
	elseif instance:IsA("WrapLayer") or instance:IsA("WrapTarget") then
		safeSet(instance, "CageMeshId", descriptorContent(properties.CageMeshId))
		safeSet(instance, "ReferenceMeshId", descriptorContent(properties.ReferenceMeshId))
		safeSet(instance, "Enabled", properties.Enabled ~= false)
		safeSet(instance, "Puffiness", tonumber(properties.Puffiness) or 0)
		safeSet(instance, "Order", tonumber(properties.Order) or 0)
	end
end

local function applyDescriptorLink(instance, node, instances)
	local properties = type(node.properties) == "table" and node.properties or {}
	if instance:IsA("Weld") or instance:IsA("Motor6D") or instance:IsA("WeldConstraint") then
		local part0 = properties.Part0 and instances[properties.Part0] or nil
		local part1 = properties.Part1 and instances[properties.Part1] or nil
		if part0 and part0:IsA("BasePart") then
			safeSet(instance, "Part0", part0)
		end
		if part1 and part1:IsA("BasePart") then
			safeSet(instance, "Part1", part1)
		end
		if instance:IsA("Weld") or instance:IsA("Motor6D") then
			safeSet(instance, "C0", descriptorCFrame(properties.C0))
			safeSet(instance, "C1", descriptorCFrame(properties.C1))
		end
		if instance:IsA("Motor6D") then
			safeSet(instance, "Transform", descriptorCFrame(properties.Transform))
		end
		safeSet(instance, "Enabled", properties.Enabled ~= false)
	end
end

local function descriptorAnchorCandidates(descriptor)
	local candidates = {}
	if type(descriptor) == "table" and type(descriptor.anchor_candidates) == "table" then
		for _, name in ipairs(descriptor.anchor_candidates) do
			if type(name) == "string" and name ~= "" then
				table.insert(candidates, name)
			end
		end
	end
	if type(descriptor) == "table" and type(descriptor.anchor) == "string" and descriptor.anchor ~= "" then
		table.insert(candidates, descriptor.anchor)
	end
	return candidates
end

local function findCharacterPart(character, candidates)
	if character == nil then
		return nil
	end
	local aliases = {
		UpperTorso = { "UpperTorso", "Torso", "HumanoidRootPart" },
		LowerTorso = { "LowerTorso", "Torso", "HumanoidRootPart", "UpperTorso" },
		Torso = { "Torso", "UpperTorso", "LowerTorso", "HumanoidRootPart" },
		Head = { "Head", "UpperTorso", "Torso", "HumanoidRootPart" },
		HumanoidRootPart = { "HumanoidRootPart", "LowerTorso", "UpperTorso", "Torso", "Head" },
		RightHand = { "RightHand", "Right Arm", "RightLowerArm", "RightUpperArm", "UpperTorso", "Torso" },
		LeftHand = { "LeftHand", "Left Arm", "LeftLowerArm", "LeftUpperArm", "UpperTorso", "Torso" },
		RightLowerArm = { "RightLowerArm", "Right Arm", "RightUpperArm", "RightHand", "UpperTorso", "Torso" },
		LeftLowerArm = { "LeftLowerArm", "Left Arm", "LeftUpperArm", "LeftHand", "UpperTorso", "Torso" },
		RightUpperArm = { "RightUpperArm", "Right Arm", "RightLowerArm", "RightHand", "UpperTorso", "Torso" },
		LeftUpperArm = { "LeftUpperArm", "Left Arm", "LeftLowerArm", "LeftHand", "UpperTorso", "Torso" },
		RightFoot = { "RightFoot", "Right Leg", "RightLowerLeg", "RightUpperLeg", "LowerTorso", "Torso" },
		LeftFoot = { "LeftFoot", "Left Leg", "LeftLowerLeg", "LeftUpperLeg", "LowerTorso", "Torso" },
		RightLowerLeg = { "RightLowerLeg", "Right Leg", "RightUpperLeg", "RightFoot", "LowerTorso", "Torso" },
		LeftLowerLeg = { "LeftLowerLeg", "Left Leg", "LeftUpperLeg", "LeftFoot", "LowerTorso", "Torso" },
		RightUpperLeg = { "RightUpperLeg", "Right Leg", "RightLowerLeg", "RightFoot", "LowerTorso", "Torso" },
		LeftUpperLeg = { "LeftUpperLeg", "Left Leg", "LeftLowerLeg", "LeftFoot", "LowerTorso", "Torso" },
	}
	local tried = {}
	local function tryName(name)
		if tried[name] then
			return nil
		end
		tried[name] = true
		local part = character:FindFirstChild(name)
		if part and part:IsA("BasePart") then
			return part
		end
		return nil
	end
	for _, candidate in ipairs(candidates or {}) do
		local list = aliases[candidate] or { candidate }
		for _, name in ipairs(list) do
			local part = tryName(name)
			if part then
				return part
			end
		end
	end
	return tryName("HumanoidRootPart") or tryName("UpperTorso") or tryName("Torso") or tryName("Head")
end

local function resolveMorphAnchor(character, descriptor, fallback)
	return findCharacterPart(character, descriptorAnchorCandidates(descriptor)) or fallback
end

local SMART_BONE_DEFAULTS = {
	Damping = 0.1,
	Stiffness = 0.2,
	Inertia = 0,
	Elasticity = 0.5,
	AnchorDepth = 0,
	Gravity = Vector3.new(0, -1, 0),
	Force = Vector3.new(0, 0.2, 0),
	WindDirection = Vector3.new(-1, 0, 0),
	WindSpeed = 8,
	WindStrength = 1,
	WindInfluence = 1,
	UpdateRate = 60,
}

local function attributeValue(instance, names)
	for _, name in ipairs(names) do
		local ok, value = pcall(function()
			return instance:GetAttribute(name)
		end)
		if ok and value ~= nil then
			return value
		end
	end
	return nil
end

local function attributeNumber(instance, names, fallback, minValue, maxValue)
	local value = attributeValue(instance, names)
	local numeric = tonumber(value)
	if numeric == nil then
		return fallback
	end
	if minValue ~= nil or maxValue ~= nil then
		numeric = math.clamp(numeric, minValue or numeric, maxValue or numeric)
	end
	return numeric
end

local function attributeVector(instance, names, fallback)
	local value = attributeValue(instance, names)
	if typeof(value) == "Vector3" then
		return value
	end
	return fallback
end

local function smartBoneSettings(rootPart)
	return {
		Damping = attributeNumber(rootPart, { "Damping", "Dampen", "Dampening", "damping", "dampen" }, SMART_BONE_DEFAULTS.Damping, 0, 1),
		Stiffness = attributeNumber(rootPart, { "Stiffness", "stiffness" }, SMART_BONE_DEFAULTS.Stiffness, 0, 1),
		Inertia = attributeNumber(rootPart, { "Inertia", "inertia" }, SMART_BONE_DEFAULTS.Inertia, 0, 1),
		Elasticity = attributeNumber(rootPart, { "Elasticity", "elasticity" }, SMART_BONE_DEFAULTS.Elasticity, 0, 1),
		AnchorDepth = math.floor(attributeNumber(rootPart, { "AnchorDepth", "anchorDepth" }, SMART_BONE_DEFAULTS.AnchorDepth, 0, 12)),
		Gravity = attributeVector(rootPart, { "Gravity", "gravity" }, SMART_BONE_DEFAULTS.Gravity),
		Force = attributeVector(rootPart, { "Force", "force" }, SMART_BONE_DEFAULTS.Force),
		WindDirection = attributeVector(rootPart, { "WindDirection", "windDirection" }, SMART_BONE_DEFAULTS.WindDirection),
		WindSpeed = attributeNumber(rootPart, { "WindSpeed", "windSpeed" }, SMART_BONE_DEFAULTS.WindSpeed, 0, 30),
		WindStrength = attributeNumber(rootPart, { "WindStrength", "windStrength" }, SMART_BONE_DEFAULTS.WindStrength, 0, 10),
		WindInfluence = attributeNumber(rootPart, { "WindInfluence", "windInfluence" }, SMART_BONE_DEFAULTS.WindInfluence, 0, 10),
		UpdateRate = attributeNumber(rootPart, { "UpdateRate", "updateRate" }, SMART_BONE_DEFAULTS.UpdateRate, 1, 120),
	}
end

local function findDescendantBoneByName(rootPart, name)
	name = trimString(name)
	if name == "" then
		return nil
	end
	for _, descendant in ipairs(rootPart:GetDescendants()) do
		if descendant:IsA("Bone") and descendant.Name == name then
			return descendant
		end
	end
	return nil
end

local function childBones(bone)
	local out = {}
	for _, child in ipairs(bone:GetChildren()) do
		if child:IsA("Bone") then
			table.insert(out, child)
		end
	end
	return out
end

local function ensureTailBone(bone, parentBone)
	local existing = bone:FindFirstChild(bone.Name .. "_OverlayTail")
	if existing and existing:IsA("Bone") then
		return existing
	end
	local tail = Instance.new("Bone")
	tail.Name = bone.Name .. "_OverlayTail"
	tail.Parent = bone
	local offset = Vector3.new(0, 0.1, 0)
	if parentBone then
		local direction = bone.WorldPosition - parentBone.WorldPosition
		if direction.Magnitude > 0.001 then
			offset = bone.CFrame:VectorToObjectSpace(direction.Unit * math.max(direction.Magnitude, 0.05))
		end
	end
	tail.CFrame = CFrame.new(offset)
	return tail
end

local function appendSmartBoneParticle(tree, bone, parentIndex, depth)
	local parent = parentIndex > 0 and tree.particles[parentIndex] or nil
	local particle = {
		bone = bone,
		parentIndex = parentIndex,
		depth = depth,
		position = bone.WorldPosition,
		lastPosition = bone.WorldPosition,
		restWorld = bone.WorldCFrame,
		restLength = parent and (parent.bone.WorldPosition - bone.WorldPosition).Magnitude or 0,
		restLocal = parent and parent.bone.WorldCFrame:ToObjectSpace(bone.WorldCFrame) or CFrame.new(),
		anchored = parentIndex == 0 or depth <= tree.settings.AnchorDepth,
		phase = (#tree.particles + 1) * 0.71,
	}
	table.insert(tree.particles, particle)
	local index = #tree.particles
	local children = childBones(bone)
	if #children == 0 and parentIndex > 0 then
		table.insert(children, ensureTailBone(bone, parent and parent.bone or nil))
	end
	for _, child in ipairs(children) do
		appendSmartBoneParticle(tree, child, index, depth + 1)
	end
end

local function createSmartBoneController(rootPart)
	if rootPart == nil or not rootPart:IsA("BasePart") then
		return nil
	end
	local rootsRaw = attributeValue(rootPart, { "Roots", "roots", "Root", "root" })
	if type(rootsRaw) ~= "string" or trimString(rootsRaw) == "" then
		return nil
	end
	local settings = smartBoneSettings(rootPart)
	local controller = {
		rootPart = rootPart,
		lastRootPosition = rootPart.Position,
		settings = settings,
		accumulator = 0,
		trees = {},
	}
	for rootName in string.gmatch(rootsRaw, "([^,]+)") do
		local rootBone = findDescendantBoneByName(rootPart, rootName)
		if rootBone then
			local tree = {
				root = rootBone,
				rootPart = rootPart,
				settings = settings,
				particles = {},
			}
			appendSmartBoneParticle(tree, rootBone, 0, 0)
			if #tree.particles > 1 then
				table.insert(controller.trees, tree)
			end
		end
	end
	if #controller.trees == 0 then
		return nil
	end
	return controller
end

local function rotationBetween(fromVector, toVector)
	if fromVector.Magnitude <= 0.0001 or toVector.Magnitude <= 0.0001 then
		return CFrame.new()
	end
	local fromUnit = fromVector.Unit
	local toUnit = toVector.Unit
	local dot = math.clamp(fromUnit:Dot(toUnit), -1, 1)
	if dot > 0.999 then
		return CFrame.new()
	end
	local axis = fromUnit:Cross(toUnit)
	if axis.Magnitude <= 0.0001 then
		axis = Vector3.new(1, 0, 0)
	end
	return CFrame.fromAxisAngle(axis.Unit, math.acos(dot))
end

local function updateSmartBoneController(controller, delta)
	if controller == nil or controller.rootPart == nil or controller.rootPart.Parent == nil then
		return
	end
	local settings = controller.settings
	local frameTime = 1 / math.max(settings.UpdateRate or 60, 1)
	controller.accumulator += delta
	if controller.accumulator < frameTime then
		return
	end
	local dt = math.clamp(controller.accumulator, 1 / 240, 1 / 20)
	controller.accumulator = 0
	local rootMove = controller.rootPart.Position - controller.lastRootPosition
	controller.lastRootPosition = controller.rootPart.Position
	local gravity = (settings.Gravity + settings.Force) * dt * dt
	local now = os.clock()

	for _, tree in ipairs(controller.trees) do
		for _, particle in ipairs(tree.particles) do
			if particle.anchored then
				particle.lastPosition = particle.bone.WorldPosition
				particle.position = particle.bone.WorldPosition
			else
				local velocity = particle.position - particle.lastPosition
				particle.lastPosition = particle.position + (rootMove * settings.Inertia)
				local wind = Vector3.new()
				if settings.WindInfluence > 0 and particle.restLength > 0 then
					local t = now * settings.WindSpeed + particle.phase
					wind = Vector3.new(
						settings.WindDirection.X * math.sin(t),
						0.05 * math.sin(t * 0.63),
						settings.WindDirection.Z * math.cos(t)
					) * (settings.WindStrength * settings.WindInfluence * 0.002 * math.max(particle.depth, 1))
				end
				particle.position += velocity * (1 - settings.Damping) + gravity + rootMove * settings.Inertia + wind
			end
		end

		for _ = 1, 2 do
			for _, particle in ipairs(tree.particles) do
				local parent = particle.parentIndex > 0 and tree.particles[particle.parentIndex] or nil
				if parent and not particle.anchored then
					local restPosition = (parent.bone.WorldCFrame * particle.restLocal).Position
					particle.position = particle.position:Lerp(restPosition, math.clamp(settings.Elasticity * dt * 10, 0, 1))
					local deltaPos = particle.position - parent.position
					local length = deltaPos.Magnitude
					if length > 0.0001 then
						local target = parent.position + deltaPos.Unit * math.max(particle.restLength, 0.001)
						particle.position = particle.position:Lerp(target, math.clamp(0.65 + settings.Stiffness * 0.35, 0, 1))
					end
				end
			end
		end

		for _, particle in ipairs(tree.particles) do
			local parent = particle.parentIndex > 0 and tree.particles[particle.parentIndex] or nil
			if parent and parent.bone and parent.bone.Parent and not parent.anchored then
				local direction = particle.position - parent.position
				if direction.Magnitude > 0.0001 then
					local current = parent.bone.WorldCFrame
					local rotated = CFrame.new(parent.position) * rotationBetween(current.UpVector, direction.Unit) * current.Rotation
					safeSet(parent.bone, "WorldCFrame", current:Lerp(rotated, 0.9))
				end
			end
		end
	end
end

local function clearDescriptorMorph(handle)
	if handle and handle.descriptorMorphRoot then
		pcall(function()
			handle.descriptorMorphRoot:Destroy()
		end)
	end
	if handle then
		handle.descriptorMorphRoot = nil
		handle.descriptorMorphAssetId = nil
		handle.descriptorMorphAnchor = nil
		handle.descriptorMorphJiggle = nil
		handle.descriptorMorphSmartBones = nil
	end
end

local function applyDescriptorMorph(handle, anchorPart, components, entityId)
	if handle == nil or anchorPart == nil or type(components) ~= "table" then
		return
	end

	local morph = components.morph
	local assetId = assetIdFromComponent(morph)
	if assetId == nil or (type(morph) == "table" and morph.enabled == false) then
		clearDescriptorMorph(handle)
		return
	end

	local descriptor = descriptorForAsset(assetId)
	if descriptor == nil then
		return
	end

	if handle.descriptorMorphAssetId == assetId and handle.descriptorMorphAnchor == anchorPart and handle.descriptorMorphRoot then
		return
	end

	clearDescriptorMorph(handle)

	local root = Instance.new("Folder")
	root.Name = "OverlayMorph_" .. tostring(entityId or assetId)
	root.Parent = handle.folder or entityFolder

	local instances = {}
	local createdNodes = {}
	local jiggleBones = {}
	for _, node in ipairs(descriptor.nodes or {}) do
		local instance = createDescriptorInstance(node)
		if instance then
			instances[node.id] = instance
			table.insert(createdNodes, { node = node, instance = instance })
			local parent = instances[node.parent] or root
			instance.Parent = parent
			applyDescriptorInstanceProperties(instance, node, anchorPart)
			if instance:IsA("BasePart") then
				local weld = Instance.new("WeldConstraint")
				weld.Name = "OverlayDescriptorWeld"
				weld.Part0 = anchorPart
				weld.Part1 = instance
				weld.Parent = instance
			elseif instance:IsA("Highlight") then
				safeSet(instance, "Adornee", anchorPart.Parent or anchorPart)
			elseif instance:IsA("Bone") then
				table.insert(jiggleBones, {
					bone = instance,
					base = safeGet(instance, "Transform", CFrame.new()),
					phase = (#jiggleBones + 1) * 0.73,
					speed = 4 + (#jiggleBones % 4),
					amplitude = 0.035,
				})
			end
		end
	end
	for _, created in ipairs(createdNodes) do
		applyDescriptorLink(created.instance, created.node, instances)
	end

	handle.descriptorMorphRoot = root
	handle.descriptorMorphAssetId = assetId
	handle.descriptorMorphAnchor = anchorPart
	if descriptor.jiggle == true and #jiggleBones > 0 then
		handle.descriptorMorphJiggle = jiggleBones
	else
		handle.descriptorMorphJiggle = nil
	end
end

local function appearanceUserIdFor(components)
	local avatar = components and components.avatar
	if type(avatar) ~= "table" then
		return nil
	end
	local userId = tonumber(avatar.appearance_user_id)
	if userId and userId > 0 then
		return math.floor(userId)
	end
	return nil
end

local function clearAppearanceAvatar(handle)
	if handle and handle.appearanceModel then
		pcall(function()
			handle.appearanceModel:Destroy()
		end)
	end
	if handle then
		handle.appearanceModel = nil
		handle.appearanceUserId = nil
		handle.appearanceAnchor = nil
		handle.appearanceLoadingId = nil
	end
end

local function prepareAppearanceModel(model)
	for _, descendant in ipairs(model:GetDescendants()) do
		if descendant:IsA("BasePart") then
			descendant.CanCollide = false
			descendant.CanTouch = false
			descendant.CanQuery = false
			descendant.Massless = true
		elseif descendant:IsA("Script") or descendant:IsA("LocalScript") then
			descendant:Destroy()
		end
	end
	local humanoid = model:FindFirstChildOfClass("Humanoid")
	if humanoid then
		pcall(function()
			humanoid.DisplayDistanceType = Enum.HumanoidDisplayDistanceType.None
		end)
	end
end

local function appearanceRootPart(model)
	return model:FindFirstChild("HumanoidRootPart")
		or model.PrimaryPart
		or model:FindFirstChildWhichIsA("BasePart")
end

local function createAppearanceModelFromUserId(userId)
	local ok, model = pcall(function()
		return Players:CreateHumanoidModelFromUserId(userId)
	end)
	if ok and model then
		return model
	end

	local descriptionOk, description = pcall(function()
		return Players:GetHumanoidDescriptionFromUserId(userId)
	end)
	if not descriptionOk or not description then
		return nil
	end

	local modelOk, fallbackModel = pcall(function()
		return Players:CreateHumanoidModelFromDescription(description, Enum.HumanoidRigType.R15)
	end)
	if modelOk then
		return fallbackModel
	end
	return nil
end

local function setProxyVisualHidden(handle, hidden)
	if handle == nil or handle.model == nil then
		return
	end
	local transparency = hidden and 1 or 0.08
	handle.model.Transparency = transparency
	if handle.proxyParts then
		for _, part in ipairs(handle.proxyParts) do
			if part then
				part.Transparency = hidden and 1 or 0.1
			end
		end
	end
	if handle.label then
		handle.label.Visible = not hidden
	end
end

local function applyAppearanceAvatar(handle, anchorPart, components, entityId)
	if handle == nil or anchorPart == nil or type(components) ~= "table" then
		return
	end

	local userId = appearanceUserIdFor(components)
	if not userId then
		clearAppearanceAvatar(handle)
		setProxyVisualHidden(handle, false)
		return
	end

	if handle.appearanceModel and handle.appearanceUserId == userId and handle.appearanceAnchor == anchorPart then
		return
	end
	if handle.appearanceLoadingId == userId then
		return
	end

	clearAppearanceAvatar(handle)
	handle.appearanceLoadingId = userId

	task.spawn(function()
		local model = createAppearanceModelFromUserId(userId)
		if not model then
			if handle.appearanceLoadingId == userId then
				handle.appearanceLoadingId = nil
				emitBridgeError("avatar.appearance_failed", "Could not load avatar appearance: " .. tostring(userId))
			end
			return
		end

		if handle.appearanceLoadingId ~= userId then
			model:Destroy()
			return
		end

		prepareAppearanceModel(model)
		local rootPart = appearanceRootPart(model)
		if not rootPart then
			model:Destroy()
			handle.appearanceLoadingId = nil
			return
		end

		model.Name = "OverlayAppearance_" .. tostring(entityId or userId)
		model.PrimaryPart = rootPart
		model.Parent = handle.folder or entityFolder
		rootPart.CFrame = anchorPart.CFrame

		local weld = Instance.new("WeldConstraint")
		weld.Name = "OverlayAppearanceWeld"
		weld.Part0 = anchorPart
		weld.Part1 = rootPart
		weld.Parent = rootPart

		handle.appearanceModel = model
		handle.appearanceUserId = userId
		handle.appearanceAnchor = anchorPart
		handle.appearanceLoadingId = nil
		setProxyVisualHidden(handle, true)
		emitBridgeEvent("avatar.appearance.ready", {
			user_id = userId,
			entity_id = entityId,
		})
	end)
end

local ProxyAvatarRenderer = {}
local NativeCharacterOverlayRenderer = {}
local OwnCharacterOverlayRenderer = {}
local Renderers = {
	proxy = ProxyAvatarRenderer,
	native = NativeCharacterOverlayRenderer,
	self = OwnCharacterOverlayRenderer,
}

local function selectRendererKind(entityId, components)
	if isOwnEntity(entityId, components) then
		return "self"
	end
	if findPlayerByRobloxUserId(robloxUserIdFor(components)) then
		return "native"
	end
	return "proxy"
end

local function configureProxyPart(part, color)
	part.Anchored = false
	part.CanCollide = false
	part.CanQuery = false
	part.Massless = true
	part.Color = color
	part.Material = Enum.Material.SmoothPlastic
	part.TopSurface = Enum.SurfaceType.Smooth
	part.BottomSurface = Enum.SurfaceType.Smooth
end

local function weldProxyPart(root, part, offset)
	part.CFrame = root.CFrame * offset
	part.Parent = root
	local weld = Instance.new("WeldConstraint")
	weld.Name = "OverlayProxyWeld"
	weld.Part0 = root
	weld.Part1 = part
	weld.Parent = part
end

local function createProxyLimb(root, name, size, offset, color, transparency)
	local part = Instance.new("Part")
	part.Name = name
	part.Size = size
	configureProxyPart(part, color)
	part.Transparency = transparency or 0
	weldProxyPart(root, part, offset)
	return part
end

local function createProxyModel(entityId, components)
	local color = colorFromComponents(entityId, components)
	local root = Instance.new("Part")
	root.Name = entityId
	root.Size = Vector3.new(1.35, 1.9, 0.7)
	configureProxyPart(root, color)
	root.Anchored = true
	root.Material = Enum.Material.Neon
	root.Transparency = 0.08

	if type(components) == "table" and type(components.transform) == "table" then
		root.CFrame = transformToCFrame(components.transform)
	else
		root.CFrame = CFrame.new(0, 5, 0)
	end
	root.Parent = entityFolder

	local proxyParts = {
		createProxyLimb(root, "Head", Vector3.new(0.88, 0.88, 0.88), CFrame.new(0, 1.38, 0), color, 0),
		createProxyLimb(root, "LeftArm", Vector3.new(0.34, 1.45, 0.42), CFrame.new(-0.93, 0.05, 0), color, 0.1),
		createProxyLimb(root, "RightArm", Vector3.new(0.34, 1.45, 0.42), CFrame.new(0.93, 0.05, 0), color, 0.1),
		createProxyLimb(root, "LeftLeg", Vector3.new(0.42, 1.25, 0.46), CFrame.new(-0.36, -1.55, 0), color, 0.1),
		createProxyLimb(root, "RightLeg", Vector3.new(0.42, 1.25, 0.46), CFrame.new(0.36, -1.55, 0), color, 0.1),
	}

	local _, label = createBillboard(root, entityId, components, Vector3.new(0, 2.45, 0))
	return root, label, proxyParts
end

local function applyEffectToPart(handle, parentPart, settings)
	if handle == nil or parentPart == nil then
		return
	end

	if handle.auraAttachment == nil or handle.auraAttachment.Parent ~= parentPart then
		if handle.auraAttachment then
			handle.auraAttachment:Destroy()
		end
		local attachment = Instance.new("Attachment")
		attachment.Name = "OverlayAuraAttachment"
		attachment.Parent = parentPart
		local particle = Instance.new("ParticleEmitter")
		particle.Name = "OverlayAuraParticles"
		particle.Texture = "rbxasset://textures/particles/sparkles_main.dds"
		particle.Lifetime = NumberRange.new(0.55, 1.2)
		particle.Speed = NumberRange.new(0.15, 0.85)
		particle.SpreadAngle = Vector2.new(360, 360)
		particle.LightEmission = 0.72
		particle.Parent = attachment
		handle.auraAttachment = attachment
		handle.auraParticle = particle
	end

	if handle.auraParticle then
		handle.auraParticle.Enabled = settings.enabled
		handle.auraParticle.Rate = settings.enabled and math.floor(4 + settings.alpha * 34) or 0
		handle.auraParticle.Color = ColorSequence.new(settings.color)
		handle.auraParticle.Size = NumberSequence.new(0.12 + settings.alpha * 0.22)
		handle.auraParticle.Transparency = NumberSequence.new(math.clamp(0.72 - settings.alpha * 0.5, 0.15, 0.9))
	end

	if settings.enabled and settings.trail then
		if handle.trailAttachment0 == nil or handle.trailAttachment0.Parent ~= parentPart then
			if handle.trailAttachment0 then handle.trailAttachment0:Destroy() end
			if handle.trailAttachment1 then handle.trailAttachment1:Destroy() end
			local a0 = Instance.new("Attachment")
			a0.Name = "OverlayTrailA"
			a0.Position = Vector3.new(-0.55, 0, 0)
			a0.Parent = parentPart
			local a1 = Instance.new("Attachment")
			a1.Name = "OverlayTrailB"
			a1.Position = Vector3.new(0.55, 0, 0)
			a1.Parent = parentPart
			handle.trailAttachment0 = a0
			handle.trailAttachment1 = a1
		end
		if handle.trail == nil then
			local trail = Instance.new("Trail")
			trail.Name = "OverlayAuraTrail"
			trail.Attachment0 = handle.trailAttachment0
			trail.Attachment1 = handle.trailAttachment1
			trail.FaceCamera = true
			trail.MinLength = 0.05
			trail.Parent = parentPart
			handle.trail = trail
		end
		handle.trail.Enabled = true
		handle.trail.Color = ColorSequence.new(settings.color)
		handle.trail.Lifetime = 0.18 + settings.alpha * 0.44
		handle.trail.Transparency = NumberSequence.new(math.clamp(0.82 - settings.alpha * 0.48, 0.25, 0.95))
	elseif handle.trail then
		handle.trail.Enabled = false
	end
end

function ProxyAvatarRenderer.onCreate(entity)
	local model, label, proxyParts = createProxyModel(entity.entityId, entity.components)
	entity.handle = { model = model, label = label, proxyParts = proxyParts }
	applyProxyAssetVisuals(entity.handle, model, entity.components)
	applyAppearanceAvatar(entity.handle, model, entity.components, entity.entityId)
	applyDescriptorMorph(entity.handle, model, entity.components, entity.entityId)
	applyEffectToPart(entity.handle, model, effectSettings(entity.entityId, entity.components))
end

function ProxyAvatarRenderer.onPatch(entity, patch)
	local handle = entity.handle
	if handle == nil then
		return
	end
	local color = colorFromComponents(entity.entityId, entity.components)
	if handle.label then
		handle.label.Text = displayNameFor(entity.entityId, entity.components)
		handle.label.TextColor3 = color
	end
	if handle.model then
		handle.model.Color = color
		if handle.proxyParts then
			for _, part in ipairs(handle.proxyParts) do
				if part then
					part.Color = color
				end
			end
		end
		if type(patch.transform) == "table" then
			handle.model.CFrame = transformToCFrame(patch.transform)
			entity.samples = {}
		end
		applyProxyAssetVisuals(handle, handle.model, entity.components)
		applyAppearanceAvatar(handle, handle.model, entity.components, entity.entityId)
		applyDescriptorMorph(handle, handle.model, entity.components, entity.entityId)
		applyEffectToPart(handle, handle.model, effectSettings(entity.entityId, entity.components))
	end
end

function ProxyAvatarRenderer.onDestroy(entity)
	destroyHandle(entity.handle)
	entity.handle = nil
end

function ProxyAvatarRenderer.onEphemeralTransform(entity, transform)
	pushSample(entity, transform)
end

local function clearNativeAttachedVisuals(handle)
	for _, key in ipairs({ "billboard", "highlight", "auraParticle", "auraAttachment", "trail", "trailAttachment0", "trailAttachment1" }) do
		if handle[key] then
			pcall(function()
				handle[key]:Destroy()
			end)
			handle[key] = nil
		end
	end
end

function OwnCharacterOverlayRenderer.onCreate(entity)
	local folder = Instance.new("Folder")
	folder.Name = entity.entityId .. "_self_overlay"
	folder.Parent = entityFolder
	entity.handle = {
		folder = folder,
		connections = {
			LocalPlayer.CharacterAdded:Connect(function()
				task.wait(0.1)
				local current = Client.entities[entity.entityId]
				if current == entity and current.rendererKind == "self" then
					OwnCharacterOverlayRenderer.onPatch(entity, {})
				end
			end),
		},
	}
	OwnCharacterOverlayRenderer.onPatch(entity, {})
end

function OwnCharacterOverlayRenderer.onPatch(entity)
	local handle = entity.handle
	if handle == nil then
		return
	end

	local character = LocalPlayer.Character
	if character == nil then
		return
	end

	local head = character:FindFirstChild("Head")
	local root = character:FindFirstChild("HumanoidRootPart")
	local anchor = head or root
	if anchor == nil then
		return
	end

	local morphAssetId = assetIdFromComponent(entity.components and entity.components.morph)
	local morphDescriptor = morphAssetId and descriptorForAsset(morphAssetId) or nil
	local morphAnchor = resolveMorphAnchor(character, morphDescriptor, anchor)

	if root then
		applyAppearanceAvatar(handle, root, entity.components, entity.entityId)
		applyEffectToPart(handle, root, effectSettings(entity.entityId, entity.components))
	else
		applyAppearanceAvatar(handle, anchor, entity.components, entity.entityId)
	end
	applyDescriptorMorph(handle, morphAnchor, entity.components, entity.entityId)
end

function OwnCharacterOverlayRenderer.onDestroy(entity)
	destroyHandle(entity.handle)
	entity.handle = nil
end

function OwnCharacterOverlayRenderer.onEphemeralTransform()
end

function NativeCharacterOverlayRenderer.onCreate(entity)
	local folder = Instance.new("Folder")
	folder.Name = entity.entityId .. "_native_overlay"
	folder.Parent = entityFolder
	entity.handle = {
		folder = folder,
		connections = {},
	}
	NativeCharacterOverlayRenderer.onPatch(entity, {})
end

function NativeCharacterOverlayRenderer.onPatch(entity)
	local handle = entity.handle
	if handle == nil then
		return
	end

	local player = findPlayerByRobloxUserId(robloxUserIdFor(entity.components))
	if player == nil or player == LocalPlayer then
		return
	end

	if handle.player ~= player then
		for _, connection in ipairs(handle.connections or {}) do
			pcall(function()
				connection:Disconnect()
			end)
		end
		handle.connections = {
			player.CharacterAdded:Connect(function()
				task.wait(0.1)
				local current = Client.entities[entity.entityId]
				if current == entity and current.rendererKind == "native" then
					NativeCharacterOverlayRenderer.onPatch(entity, {})
				end
			end),
		}
		handle.player = player
		clearNativeAttachedVisuals(handle)
	end

	local character = player.Character
	if character == nil then
		return
	end

	local head = character:FindFirstChild("Head")
	local root = character:FindFirstChild("HumanoidRootPart")
	local anchor = head or root
	if anchor == nil then
		return
	end
	local morphAssetId = assetIdFromComponent(entity.components and entity.components.morph)
	local morphDescriptor = morphAssetId and descriptorForAsset(morphAssetId) or nil
	local morphAnchor = resolveMorphAnchor(character, morphDescriptor, anchor)

	local color = colorFromComponents(entity.entityId, entity.components)
	local effect = effectSettings(entity.entityId, entity.components)

	if handle.billboard == nil or handle.billboard.Parent ~= anchor then
		if handle.billboard then
			handle.billboard:Destroy()
		end
		handle.billboard, handle.label = createBillboard(anchor, entity.entityId, entity.components, Vector3.new(0, 2.8, 0))
	end
	if handle.label then
		handle.label.Text = displayNameFor(entity.entityId, entity.components)
		handle.label.TextColor3 = color
	end

	if handle.highlight == nil and activeOverlayHighlightCount() >= CONFIG.max_native_highlights then
		-- Roblox only renders a small number of Highlight instances. Keep the
		-- cheap billboard/particle overlay when the room is crowded.
	elseif handle.highlight == nil or handle.highlight.Adornee ~= character then
		if handle.highlight then
			handle.highlight:Destroy()
		end
		local highlight = Instance.new("Highlight")
		highlight.Name = "OverlayAura"
		highlight.Adornee = character
		highlight.FillTransparency = 0.86
		highlight.OutlineTransparency = 0.12
		pcall(function()
			highlight.DepthMode = Enum.HighlightDepthMode.AlwaysOnTop
		end)
		highlight.Parent = handle.folder
		handle.highlight = highlight
	end
	if handle.highlight then
		handle.highlight.FillColor = color
		handle.highlight.OutlineColor = color
		handle.highlight.FillTransparency = math.clamp(0.95 - effect.alpha * 0.22, 0.68, 0.98)
		handle.highlight.OutlineTransparency = math.clamp(0.45 - effect.alpha * 0.3, 0.08, 0.75)
	end

	if root then
		applyAppearanceAvatar(handle, root, entity.components, entity.entityId)
		applyEffectToPart(handle, root, effect)
	else
		applyAppearanceAvatar(handle, anchor, entity.components, entity.entityId)
	end
	applyDescriptorMorph(handle, morphAnchor, entity.components, entity.entityId)
end

function NativeCharacterOverlayRenderer.onDestroy(entity)
	destroyHandle(entity.handle)
	entity.handle = nil
end

function NativeCharacterOverlayRenderer.onEphemeralTransform()
	-- Same-server native characters are already moved by Roblox. Applying network
	-- transforms here would fight the host movement and cause jitter.
end

local function ensureRenderer(entityId)
	local entity = Client.entities[entityId]
	if entity == nil then
		return
	end

	local desired = selectRendererKind(entityId, entity.components)
	if entity.rendererKind ~= desired then
		local oldRenderer = entity.rendererKind and Renderers[entity.rendererKind]
		if oldRenderer then
			oldRenderer.onDestroy(entity)
		else
			destroyHandle(entity.handle)
			entity.handle = nil
		end
		entity.rendererKind = desired
		entity.samples = {}
		if desired and Renderers[desired] then
			Renderers[desired].onCreate(entity)
		end
	elseif desired and Renderers[desired] then
		Renderers[desired].onPatch(entity, {})
	end
end

local function refreshRenderers()
	for entityId in pairs(Client.entities) do
		ensureRenderer(entityId)
	end
end

local function scheduleRendererRefresh()
	task.spawn(function()
		task.wait(0.1)
		if Client.running then
			refreshRenderers()
		end
	end)
end

trackConnection(Players.PlayerAdded:Connect(function(player)
	trackConnection(player.CharacterAdded:Connect(scheduleRendererRefresh))
	scheduleRendererRefresh()
end))

trackConnection(Players.PlayerRemoving:Connect(scheduleRendererRefresh))

for _, player in ipairs(Players:GetPlayers()) do
	trackConnection(player.CharacterAdded:Connect(scheduleRendererRefresh))
end

local function upsertEntity(entityId, components)
	prefetchAssetRefs(components)
	local entity = Client.entities[entityId]
	if entity == nil then
		entity = {
			entityId = entityId,
			components = components,
			samples = {},
		}
		Client.entities[entityId] = entity
	else
		entity.components = components
	end

	ensureRenderer(entityId)
end

local function patchEntity(entityId, patch)
	prefetchAssetRefs(patch)
	local entity = Client.entities[entityId]
	if entity == nil then
		upsertEntity(entityId, patch)
		return
	end

	entity.components = entity.components or {}
	for key, value in pairs(patch) do
		entity.components[key] = value
	end

	ensureRenderer(entityId)
	if entity.rendererKind and Renderers[entity.rendererKind] then
		Renderers[entity.rendererKind].onPatch(entity, patch)
	end
end

local function destroyEntity(entityId)
	local entity = Client.entities[entityId]
	if entity == nil then
		return
	end
	if entity.rendererKind and Renderers[entity.rendererKind] then
		Renderers[entity.rendererKind].onDestroy(entity)
	else
		destroyHandle(entity.handle)
	end
	Client.entities[entityId] = nil
end

local function clearEntities()
	for entityId in pairs(Client.entities) do
		destroyEntity(entityId)
	end
	Client.entities = {}
end

local function clearCurrentRoom(roomId)
	if Client.room and (roomId == nil or Client.room.id == roomId) then
		Client.room = nil
	end
	if roomId == nil or Client.avatarReadyRoomId == roomId then
		Client.avatarReadyRoomId = nil
	end
	if roomId == nil or Client.roomClosingId == roomId then
		Client.roomClosingId = nil
	end
	Client.joining = nil
	Client.deferredSnapshot = nil
	clearEntities()
end

-- ---------------------------------------------------------------------------
-- Remote movement interpolation (room.state -> buffered lerp)
-- ---------------------------------------------------------------------------

trackConnection(RunService.Heartbeat:Connect(function()
	local now = os.clock()
	local renderAt = os.clock() - CONFIG.interp_delay
	for entityId, entity in pairs(Client.entities) do
		local handle = entity.handle
		if entity.rendererKind == "proxy" and handle and handle.model and #entity.samples > 0 then
			local samples = entity.samples
			local previous, following
			for i = #samples, 1, -1 do
				if samples[i].at <= renderAt then
					previous = samples[i]
					following = samples[i + 1]
					break
				end
			end

			if previous == nil then
				handle.model.CFrame = samples[1].cf
			elseif following == nil then
				handle.model.CFrame = previous.cf
			else
				local span = following.at - previous.at
				local alpha = span > 0 and math.clamp((renderAt - previous.at) / span, 0, 1) or 1
				handle.model.CFrame = previous.cf:Lerp(following.cf, alpha)
			end
		end
		if handle and handle.descriptorMorphJiggle then
			for _, item in ipairs(handle.descriptorMorphJiggle) do
				if item.bone and item.bone.Parent then
					local sway = math.sin(now * item.speed + item.phase) * item.amplitude
					local twist = math.cos(now * (item.speed * 0.7) + item.phase) * item.amplitude * 0.45
					safeSet(item.bone, "Transform", item.base * CFrame.Angles(sway, 0, twist))
				end
			end
		end
	end
end))

-- ---------------------------------------------------------------------------
-- Server message handlers
-- ---------------------------------------------------------------------------

local function countKeys(map)
	local count = 0
	for _ in pairs(map or {}) do
		count += 1
	end
	return count
end

local function applySnapshot(data)
	if type(data) ~= "table" then
		return
	end
	if Client.room == nil or data.room_id ~= Client.room.id then
		Client.deferredSnapshot = data
		return
	end
	Client.deferredSnapshot = nil
	AssetCache.registerAssets(data.assets or {})
	clearEntities()
	Client.room.version = tonumber(data.version) or Client.room.version or 0
	Client.room.ownerUserId = data.owner_user_id
	for entityId, entity in pairs(data.entities or {}) do
		upsertEntity(entityId, entity.components or {})
	end
	log(("snapshot applied: version=%d entities=%d"):format(Client.room.version, countKeys(data.entities)))
	emitBridgeEvent("room.snapshot", data)
end

local function resync()
	if Client.room == nil or Client.room.id == nil then
		return
	end
	log("delta gap detected; requesting fresh snapshot")
	Client.joining = { roomId = Client.room.id, resumeFrom = 0 }
	sendMessage("room.join", mergeTables({
		room_id = Client.room.id,
		resume_from_version = 0,
	}, currentRobloxJoinContext()))
end

local function applyDelta(data)
	if type(data) ~= "table" then
		return
	end
	if Client.room == nil or data.room_id ~= Client.room.id then
		return
	end
	if data.base_version ~= Client.room.version then
		resync()
		return
	end

	Client.room.version = data.version
	for _, op in ipairs(data.ops or {}) do
		if op.op == "entity.create" then
			upsertEntity(op.entity_id, op.components or {})
		elseif op.op == "component.patch" then
			patchEntity(op.entity_id, op.components or {})
		elseif op.op == "entity.destroy" then
			destroyEntity(op.entity_id)
		end
	end
end

local function applyRoomState(data)
	if type(data) ~= "table" then
		return
	end
	if Client.room == nil or data.room_id ~= Client.room.id then
		return
	end
	for _, state in ipairs(data.states or {}) do
		if state.entity_id ~= Client.ownAvatarId then
			local entity = Client.entities[state.entity_id]
			local renderer = entity and entity.rendererKind and Renderers[entity.rendererKind]
			if renderer and type(state.transform) == "table" then
				renderer.onEphemeralTransform(entity, state.transform)
			end
		end
	end
end

local function handleServerMessage(raw)
	local ok, message = pcall(HttpService.JSONDecode, HttpService, raw)
	if not ok or type(message) ~= "table" then
		return
	end
	if type(message.data) ~= "table" then
		message.data = {}
	end

	Client.lastServerContact = os.clock()

	if message.t == "room.joined" and type(message.data) == "table" then
		Client.room = Client.room or {}
		Client.room.id = message.data.room_id
		Client.room.ownerUserId = message.data.owner_user_id
		local resumeFrom = 0
		if Client.joining and Client.joining.roomId == message.data.room_id then
			resumeFrom = Client.joining.resumeFrom or 0
		end
		if resumeFrom > 0 then
			Client.room.version = Client.room.version or resumeFrom
		else
			Client.room.version = message.data.current_version or Client.room.version or 0
		end
		if Client.deferredSnapshot and Client.deferredSnapshot.room_id == Client.room.id then
			applySnapshot(Client.deferredSnapshot)
		end
		emitBridgeEvent("room.join", {
			room_id = Client.room.id,
			current_version = Client.room.version,
			roblox_route = message.data.roblox_route,
		})
		Client.joining = nil
	end

	if message.t == "room.deleted" and type(message.data) == "table" then
		if Client.room and Client.room.id == message.data.room_id then
			clearCurrentRoom(message.data.room_id)
		end
		emitBridgeEvent("room.deleted", message.data)
	end

	if message.t == "room.kicked" and type(message.data) == "table" then
		if Client.room and Client.room.id == message.data.room_id then
			clearCurrentRoom(message.data.room_id)
		end
		emitBridgeEvent("room.kicked", message.data)
	end

	local requestId = message.data and message.data.request_id
	if requestId and Client.pending[requestId] then
		Client.pending[requestId].callback(message)
		return
	end

	if message.t == "room.snapshot" then
		applySnapshot(message.data)
	elseif message.t == "room.delta" then
		applyDelta(message.data)
	elseif message.t == "room.state" then
		applyRoomState(message.data)
	elseif message.t == "room.member.joined" then
		log("member joined:", message.data.display_name or message.data.user_id)
		emitBridgeEvent("room.member.joined", message.data)
	elseif message.t == "room.member.left" then
		log("member left:", message.data.user_id)
		emitBridgeEvent("room.member.left", message.data)
	elseif message.t == "room.invited" then
		emitBridgeEvent("room.invited", message.data)
	elseif message.t == "chat.message" then
		log(("[chat][%s] %s: %s"):format(tostring(message.data.scope or "?"), tostring(message.data.display_name or message.data.user_id or "?"), tostring(message.data.text or "")))
		if message.data.scope == "global" then
			emitBridgeEvent("chat.global", message.data)
		else
			emitBridgeEvent("chat.room", message.data)
		end
	elseif message.t == "error" then
		log("server error:", message.data and message.data.code, message.data and message.data.message)
		emitBridgeError(message.data and message.data.code, message.data and message.data.message)
	end
end

-- ---------------------------------------------------------------------------
-- Session flow: hello -> auth -> join -> avatar -> move loop
-- ---------------------------------------------------------------------------

local function resolveRoom()
	-- Reconnect path: rejoin the known room and resume from the last applied version.
	if Client.room and Client.room.id then
		return Client.room.id, Client.room.version
	end

	local pendingRoomId = pendingRoomFromTeleportSetting()
	if pendingRoomId then
		return pendingRoomId, 0
	end

	local listResponse, listError = awaitRequest("room.list", {})
	if listResponse == nil then
		return nil, nil, "room.list failed: " .. tostring(listError)
	end
	Client.cachedRooms = listResponse.data.rooms or {}
	emitBridgeEvent("room.list", { rooms = Client.cachedRooms })

	for _, room in ipairs(Client.cachedRooms) do
		if room.name == CONFIG.room_name then
			return room.room_id, 0
		end
	end

	local createResponse, createError = awaitRequest("room.create", mergeTables({
		name = CONFIG.room_name,
		visibility = "public",
		max_members = 50,
	}, currentRobloxCreateRoute()))
	if createResponse == nil then
		return nil, nil, "room.create failed: " .. tostring(createError)
	end
	return createResponse.data.room_id, 0
end

local function ensureOwnAvatarEntity(roomId)
	if not roomId then
		return nil, "room id missing"
	end
	if Client.avatarReadyRoomId == roomId and Client.ownAvatarId then
		return Client.ownAvatarId
	end

	Client.avatarReadyRoomId = nil
	local avatarResponse, avatarError = awaitRequest("cmd.avatar.set", {
		room_id = roomId,
		avatar = {
			display_name = LocalPlayer.DisplayName,
			roblox_user_id = LocalPlayer.UserId,
			roblox_name = LocalPlayer.Name,
			render_surface = "native_character_overlay",
			overlay_color = colorToArray(colorFromId(tostring(LocalPlayer.UserId))),
		},
	})
	if avatarResponse == nil then
		return nil, "cmd.avatar.set failed: " .. tostring(avatarError)
	end

	Client.ownAvatarId = avatarResponse.data.entity_id
	Client.avatarReadyRoomId = roomId
	return Client.ownAvatarId
end

local function requestRoomJoin(roomId, resumeFrom)
	Client.avatarReadyRoomId = nil
	Client.joining = { roomId = roomId, resumeFrom = resumeFrom or 0 }
	local joinResponse, joinError = awaitRequest("room.join", mergeTables({
		room_id = roomId,
		resume_from_version = resumeFrom or 0,
	}, currentRobloxJoinContext()))
	if joinResponse ~= nil and type(joinResponse.data) == "table" then
		Client.room = Client.room or {}
		Client.room.id = joinResponse.data.room_id
		if (resumeFrom or 0) <= 0 then
			Client.room.version = joinResponse.data.current_version or Client.room.version or 0
		end
		if joinResponse.data.teleport_required then
			local okTeleport, teleportError = tryTeleportToRoute(joinResponse.data.teleport_route or joinResponse.data.roblox_route, joinResponse.data.room_id)
			if okTeleport then
				return joinResponse, "teleporting"
			end
			log("teleport failed:", teleportError)
		end
	end
	Client.joining = nil
	return joinResponse, joinError
end

local OverlayBridge = {}

function OverlayBridge.onEvent(callback)
	if type(callback) ~= "function" then
		return function() end
	end

	table.insert(Client.bridgeSubscribers, callback)
	local active = true
	return function()
		if not active then
			return
		end
		active = false
		for index, existing in ipairs(Client.bridgeSubscribers) do
			if existing == callback then
				table.remove(Client.bridgeSubscribers, index)
				break
			end
		end
	end
end

OverlayBridge.OnEvent = OverlayBridge.onEvent

function OverlayBridge.getState()
	return {
		connected = Client.connected,
		user = Client.user,
		room = Client.room,
		rooms = Client.cachedRooms,
		assets = Client.assetCatalog,
	}
end

local function bridgeListRooms()
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		local response, err = awaitRequest("room.list", {})
		if response == nil then
			emitBridgeError("room.list_failed", tostring(err))
			return
		end
		Client.cachedRooms = response.data.rooms or {}
		emitBridgeEvent("room.list", {
			rooms = Client.cachedRooms,
			online_count = response.data.online_count,
		})
	end)
end

local function bridgeJoinRoom(roomId)
	roomId = trimString(roomId)
	if roomId == "" then
		emitBridgeError("room.required", "Select a room first")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		local response, err = requestRoomJoin(roomId, 0)
		if response == nil then
			emitBridgeError("room.join_failed", tostring(err))
			return
		end
		if Client.teleporting then
			return
		end
		local _, avatarError = ensureOwnAvatarEntity(roomId)
		if avatarError then
			emitBridgeError("avatar.set_failed", tostring(avatarError))
			return
		end
		emitBridgeState("joined " .. tostring(roomId))
	end)
end

local function bridgeCreateRoom(name)
	name = trimString(name)
	if name == "" then
		emitBridgeError("room.name_required", "Room name is required")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		local listResponse = awaitRequest("room.list", {})
		if listResponse and listResponse.data then
			Client.cachedRooms = listResponse.data.rooms or {}
			for _, room in ipairs(Client.cachedRooms) do
				if room.name == name and Client.user and room.owner_user_id == Client.user.user_id then
					bridgeJoinRoom(room.room_id)
					emitBridgeEvent("room.list", { rooms = Client.cachedRooms })
					return
				end
			end
		end

		local response, err = awaitRequest("room.create", mergeTables({
			name = name,
			visibility = "public",
			max_members = 50,
		}, currentRobloxCreateRoute()))
		if response == nil then
			emitBridgeError("room.create_failed", tostring(err))
			return
		end

		bridgeJoinRoom(response.data.room_id)
		bridgeListRooms()
	end)
end

local function bridgeDeleteRoom(roomId)
	roomId = trimString(roomId)
	if roomId == "" then
		emitBridgeError("room.required", "Select a room first")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		if Client.room and Client.room.id == roomId then
			Client.roomClosingId = roomId
			Client.avatarReadyRoomId = nil
		end
		local response, err = awaitRequest("room.delete", { room_id = roomId })
		if response == nil then
			if Client.roomClosingId == roomId then
				Client.roomClosingId = nil
			end
			emitBridgeError("room.delete_failed", tostring(err))
			return
		end
		bridgeListRooms()
	end)
end

local function bridgeInviteRoom(userId, roomId)
	userId = trimString(userId)
	roomId = trimString(roomId) ~= "" and trimString(roomId) or (Client.room and Client.room.id)
	if userId == "" then
		emitBridgeError("room.invite_user_required", "User id is required")
		return
	end
	if not roomId or roomId == "" then
		emitBridgeError("room.required", "Join a room before inviting")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		local response, err = awaitRequest("room.invite", {
			room_id = roomId,
			user_id = userId,
		})
		if response == nil then
			emitBridgeError("room.invite_failed", tostring(err))
			return
		end
		emitBridgeState("invited " .. tostring(userId))
	end)
end

local function bridgeKickRoom(userId, roomId)
	userId = trimString(userId)
	roomId = trimString(roomId) ~= "" and trimString(roomId) or (Client.room and Client.room.id)
	if userId == "" then
		emitBridgeError("room.kick_user_required", "User id is required")
		return
	end
	if not roomId or roomId == "" then
		emitBridgeError("room.required", "Join a room before kicking")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	task.spawn(function()
		local response, err = awaitRequest("room.kick", {
			room_id = roomId,
			user_id = userId,
		})
		if response == nil then
			emitBridgeError("room.kick_failed", tostring(err))
			return
		end
		emitBridgeState("kicked " .. tostring(userId))
	end)
end

local function bridgeSendChat(scope, text, roomId)
	text = trimString(text)
	if text == "" then
		emitBridgeError("chat.empty", "Message is empty")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end

	local outgoing = {
		scope = scope == "global" and "global" or "room",
		text = text,
	}
	if outgoing.scope == "room" then
		outgoing.room_id = trimString(roomId) ~= "" and trimString(roomId) or (Client.room and Client.room.id)
		if not outgoing.room_id then
			emitBridgeError("room.required", "Join a room before sending room chat")
			return
		end
	end
	sendMessage("chat.send", outgoing)
end

local function isHttpUrl(value)
	value = string.lower(trimString(value))
	return string.sub(value, 1, 7) == "http://" or string.sub(value, 1, 8) == "https://"
end

local function assetTypeForBridge(kind, url)
	local cleanUrl = string.lower(tostring(url or ""))
	if string.find(cleanUrl, "%.png", 1, false)
		or string.find(cleanUrl, "%.jpg", 1, false)
		or string.find(cleanUrl, "%.jpeg", 1, false)
		or string.find(cleanUrl, "%.webp", 1, false) then
		return "texture"
	end
	if string.find(cleanUrl, "%.mesh", 1, false) then
		return "mesh"
	end
	if kind == "animation" then
		return "animation"
	end
	return kind .. "_bundle"
end

local function assetFormatForBridge(kind, url)
	local cleanUrl = string.lower(tostring(url or ""))
	if string.find(cleanUrl, "%.json", 1, false) then
		return "overlay_json_v1"
	end
	if string.find(cleanUrl, "%.rbxmx", 1, false) then
		return "rbxmx"
	end
	return nil
end

local function assetIdFromUrl(kind, url)
	local cleanUrl = string.match(tostring(url or ""), "^[^%?]+") or tostring(url or "")
	local fileName = string.match(cleanUrl, "([^/\\]+)$") or cleanUrl
	fileName = string.gsub(fileName, "%.[%w]+$", "")
	local assetId = safeFileName(kind .. "_" .. fileName)
	if assetId == "" or assetId == kind .. "_" then
		assetId = kind .. "_" .. safeFileName(tostring(os.time()))
	end
	return string.sub(assetId, 1, 120)
end

local function bridgePayloadTable(payload)
	if type(payload) == "table" then
		return payload
	end
	return { value = payload }
end

local function parseAppearanceUserId(payload)
	payload = bridgePayloadTable(payload)
	local raw = trimString(payload.appearance_user_id or payload.appearanceUserId or payload.roblox_avatar_user_id)
	if raw == "" then
		raw = trimString(payload.value or payload.input)
	end
	if raw == "" or isHttpUrl(raw) then
		return nil
	end

	local lower = string.lower(raw)
	if string.sub(lower, 1, 5) == "user:" then
		raw = string.sub(raw, 6)
	end

	if string.match(raw, "^%d+$") then
		local value = tonumber(raw)
		if value and value > 0 then
			return math.floor(value)
		end
	end
	return nil
end

local function appearanceInputRaw(payload)
	payload = bridgePayloadTable(payload)
	local raw = trimString(payload.appearance_user_id or payload.appearanceUserId or payload.roblox_avatar_user_id)
	if raw == "" then
		raw = trimString(payload.value or payload.input)
	end
	return raw
end

local function resolveAppearanceUserId(payload)
	local numericId = parseAppearanceUserId(payload)
	if numericId then
		return numericId
	end

	local raw = appearanceInputRaw(payload)
	if raw == "" or isHttpUrl(raw) then
		return nil
	end

	local lower = string.lower(raw)
	if string.sub(lower, 1, 5) == "user:" then
		raw = string.sub(raw, 6)
	elseif string.sub(lower, 1, 5) == "name:" then
		raw = string.sub(raw, 6)
	elseif string.sub(raw, 1, 1) == "@" then
		raw = string.sub(raw, 2)
	end
	raw = trimString(raw)

	if raw == "" or string.match(raw, "[/%\\:%?]") then
		return nil
	end

	local ok, userId = pcall(function()
		return Players:GetUserIdFromNameAsync(raw)
	end)
	if ok and tonumber(userId) then
		return tonumber(userId)
	end
	return nil
end

local function resolveBridgeAsset(kind, payload)
	payload = bridgePayloadTable(payload)
	local value = trimString(payload.value or payload.input)
	local url = trimString(payload.url)
	local assetId = trimString(payload.asset_id or payload.assetId or payload.id)

	if url == "" and isHttpUrl(value) then
		url = value
	elseif assetId == "" and value ~= "" then
		assetId = value
	end

	if url ~= "" then
		if assetId == "" then
			assetId = assetIdFromUrl(kind, url)
		end
		local hash = trimString(payload.hash)
		if hash == "" then
			hash = "unsafe:" .. assetId
		end
		local asset = {
			asset_id = assetId,
			type = trimString(payload.type) ~= "" and trimString(payload.type) or assetTypeForBridge(kind, url),
			kind = trimString(payload.kind) ~= "" and trimString(payload.kind) or assetTypeForBridge(kind, url),
			format = trimString(payload.format) ~= "" and trimString(payload.format) or assetFormatForBridge(kind, url),
			entrypoint = trimString(payload.entrypoint) ~= "" and trimString(payload.entrypoint) or nil,
			preview = trimString(payload.preview) ~= "" and trimString(payload.preview) or nil,
			version = tonumber(payload.version) or 1,
			hash = hash,
			url = url,
			source = trimString(payload.source) ~= "" and trimString(payload.source) or "runtime-ui",
		}
		local size = tonumber(payload.size)
		if size then
			asset.size = math.floor(size)
		end

		local response, err = awaitRequest("asset.register", asset, CONFIG.request_timeout + 5)
		if response == nil then
			return nil, "asset.register failed: " .. tostring(err)
		end
		local registered = response.data and response.data.asset or asset
		AssetCache.registerAssets({ registered })
		emitBridgeEvent("asset.registered", { asset = registered })
		return registered
	end

	if assetId == "" then
		return nil
	end

	local catalogAsset = Client.assetCatalogById[assetId]
	if catalogAsset and type(catalogAsset.url) == "string" and catalogAsset.url ~= "" then
		return resolveBridgeAsset(kind, catalogAsset)
	end

	local response, err = awaitRequest("asset.get", { asset_id = assetId }, CONFIG.request_timeout)
	if response == nil then
		return nil, "asset.get failed: " .. tostring(err)
	end
	local asset = response.data and response.data.asset
	if asset == nil then
		return nil, "asset.get returned no asset"
	end
	AssetCache.registerAssets({ asset })
	return asset
end

local function bridgeRequireRoom(action)
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return nil
	end
	if not Client.room or not Client.room.id then
		emitBridgeError("room.required", "Join a room before " .. action)
		return nil
	end
	return Client.room.id
end

local function bridgeSetAvatar(payload)
	payload = bridgePayloadTable(payload)
	local roomId = bridgeRequireRoom("setting avatar")
	if not roomId then
		return
	end

	task.spawn(function()
		local appearanceUserId = resolveAppearanceUserId(payload)
		local asset, assetError = nil, nil
		if not appearanceUserId then
			asset, assetError = resolveBridgeAsset("avatar", payload)
		end
		if assetError then
			emitBridgeError("asset.resolve_failed", assetError)
			return
		end

		local overlayColor = payload.color
		if type(overlayColor) ~= "table" then
			overlayColor = colorToArray(colorFromId(tostring(LocalPlayer.UserId)))
		end

		local avatar = {
			display_name = trimString(payload.display_name or payload.displayName) ~= "" and trimString(payload.display_name or payload.displayName) or LocalPlayer.DisplayName,
			roblox_user_id = LocalPlayer.UserId,
			roblox_name = LocalPlayer.Name,
			render_surface = "native_character_overlay",
			overlay_color = overlayColor,
		}
		if asset then
			avatar.asset_id = asset.asset_id
			avatar.asset_type = asset.type
		end
		if appearanceUserId then
			avatar.appearance_user_id = appearanceUserId
		end

		local response, err = awaitRequest("cmd.avatar.set", {
			room_id = roomId,
			avatar = avatar,
		})
		if response == nil then
			emitBridgeError("avatar.set_failed", tostring(err))
			return
		end

		Client.ownAvatarId = response.data.entity_id
		Client.avatarReadyRoomId = roomId
		emitBridgeEvent("avatar.applied", {
			room_id = roomId,
			entity_id = Client.ownAvatarId,
			asset_id = asset and asset.asset_id or nil,
			appearance_user_id = appearanceUserId,
		})
		if asset then
			emitBridgeState("avatar asset applied: " .. tostring(asset.asset_id))
		elseif appearanceUserId then
			emitBridgeState("avatar appearance id applied: " .. tostring(appearanceUserId))
		else
			emitBridgeState("avatar reset")
		end
	end)
end

local function bridgeApplyMorph(payload)
	payload = bridgePayloadTable(payload)
	local roomId = bridgeRequireRoom("applying morph")
	if not roomId then
		return
	end

	task.spawn(function()
		local asset, assetError = resolveBridgeAsset("morph", payload)
		if assetError then
			emitBridgeError("asset.resolve_failed", assetError)
			return
		end

		local preset = trimString(payload.preset or payload.name)
		local morph = {}
		if asset then
			morph.asset_id = asset.asset_id
			morph.asset_type = asset.type
		elseif preset ~= "" and string.lower(preset) ~= "none" then
			morph.preset = preset
		else
			morph.preset = "None"
			morph.enabled = false
		end

		local response, err = awaitRequest("cmd.morph.apply", {
			room_id = roomId,
			entity_id = Client.ownAvatarId,
			morph = morph,
		})
		if response == nil then
			emitBridgeError("morph.apply_failed", tostring(err))
			return
		end

		Client.ownAvatarId = response.data.entity_id
		Client.avatarReadyRoomId = roomId
		emitBridgeEvent("morph.applied", {
			room_id = roomId,
			entity_id = Client.ownAvatarId,
			asset_id = asset and asset.asset_id or nil,
			preset = morph.preset,
		})
		emitBridgeState(asset and ("morph asset applied: " .. tostring(asset.asset_id)) or ("morph applied: " .. tostring(morph.preset)))
	end)
end

local function bridgePlayAnimation(payload)
	payload = bridgePayloadTable(payload)
	local roomId = bridgeRequireRoom("playing animation")
	if not roomId then
		return
	end

	task.spawn(function()
		local asset, assetError = resolveBridgeAsset("animation", payload)
		if assetError then
			emitBridgeError("asset.resolve_failed", assetError)
			return
		end

		local animation = {
			name = trimString(payload.name or payload.preset or payload.value) ~= "" and trimString(payload.name or payload.preset or payload.value) or "Idle",
			intensity = tonumber(payload.intensity) or 50,
		}
		if asset then
			animation.asset_id = asset.asset_id
			animation.asset_type = asset.type
		end

		local response, err = awaitRequest("cmd.animation.play", {
			room_id = roomId,
			entity_id = Client.ownAvatarId,
			animation = animation,
		})
		if response == nil then
			emitBridgeError("animation.play_failed", tostring(err))
			return
		end

		emitBridgeEvent("animation.played", {
			room_id = roomId,
			entity_id = response.data.entity_id,
			asset_id = asset and asset.asset_id or nil,
			name = animation.name,
		})
		emitBridgeState("animation played: " .. tostring(animation.name))
	end)
end

local function bridgePatchEffect(effect)
	if type(effect) ~= "table" then
		emitBridgeError("effect.invalid", "Effect payload is required")
		return
	end
	if not Client.connected then
		emitBridgeError("runtime.disconnected", "Runtime is not connected")
		return
	end
	if not Client.room or not Client.room.id then
		emitBridgeError("room.required", "Join a room before applying effects")
		return
	end

	task.spawn(function()
		local _, avatarError = ensureOwnAvatarEntity(Client.room.id)
		if avatarError then
			emitBridgeError("avatar.set_failed", tostring(avatarError))
			return
		end

		local response, err = awaitRequest("cmd.entity.patch", {
			room_id = Client.room.id,
			entity_id = Client.ownAvatarId,
			components = {
				effect = effect,
			},
		})
		if response == nil then
			emitBridgeError("effect.patch_failed", tostring(err))
			return
		end
		emitBridgeState("effect applied")
	end)
end

local function localPreviewAnchor()
	local character = LocalPlayer.Character
	if character == nil then
		return nil
	end
	return character:FindFirstChild("HumanoidRootPart") or character:FindFirstChild("Head")
end

local function clearLocalPreview()
	if Client.previewHandle then
		destroyHandle(Client.previewHandle)
		Client.previewHandle = nil
	end
end

local function bridgePreviewEffect(effect)
	if type(effect) ~= "table" then
		emitBridgeError("effect.invalid", "Effect payload is required")
		return
	end
	local anchor = localPreviewAnchor()
	if anchor == nil then
		emitBridgeError("preview.no_character", "Local character is not ready")
		return
	end

	if Client.previewHandle == nil then
		Client.previewHandle = {}
	end
	applyEffectToPart(Client.previewHandle, anchor, effectSettings("local_preview", {
		effect = effect,
		avatar = { overlay_color = effect.color },
	}))
	emitBridgeState("preview applied")
end

function OverlayBridge.send(eventName, payload)
	payload = payload or {}
	if eventName == "room.list" then
		bridgeListRooms()
	elseif eventName == "room.create" then
		bridgeCreateRoom(payload.name)
	elseif eventName == "room.join" then
		bridgeJoinRoom(payload.room_id)
	elseif eventName == "room.delete" then
		bridgeDeleteRoom(payload.room_id)
	elseif eventName == "room.invite" then
		bridgeInviteRoom(payload.user_id, payload.room_id)
	elseif eventName == "room.kick" then
		bridgeKickRoom(payload.user_id, payload.room_id)
	elseif eventName == "chat.send" then
		bridgeSendChat(payload.scope, payload.text, payload.room_id)
	elseif eventName == "cmd.avatar.set" then
		bridgeSetAvatar(payload)
	elseif eventName == "cmd.morph.apply" then
		bridgeApplyMorph(payload)
	elseif eventName == "cmd.animation.play" then
		bridgePlayAnimation(payload)
	elseif eventName == "asset.catalog.load" then
		loadAssetCatalog(payload.url or payload.catalog_url or payload.value)
	elseif eventName == "cmd.entity.patch" then
		bridgePatchEffect(payload.effect or payload.components and payload.components.effect or payload)
	elseif eventName == "effect.preview" then
		bridgePreviewEffect(payload.effect or payload)
	elseif eventName == "diagnostics.ping" then
		sendMessage("ping", { client_time_ms = math.floor(os.clock() * 1000) })
	elseif eventName == "OverlayStop" then
		if type(GLOBAL.OverlayStop) == "function" then
			GLOBAL.OverlayStop()
		end
	else
		emitBridgeError("bridge.unknown_event", "Unknown bridge event: " .. tostring(eventName))
	end
end

OverlayBridge.emit = OverlayBridge.send
OverlayBridge.listRooms = bridgeListRooms
OverlayBridge.createRoom = bridgeCreateRoom
OverlayBridge.joinRoom = bridgeJoinRoom
OverlayBridge.deleteRoom = bridgeDeleteRoom
OverlayBridge.inviteRoom = bridgeInviteRoom
OverlayBridge.kickRoom = bridgeKickRoom
OverlayBridge.sendChat = bridgeSendChat
OverlayBridge.setAvatar = bridgeSetAvatar
OverlayBridge.applyMorph = bridgeApplyMorph
OverlayBridge.playAnimation = bridgePlayAnimation
OverlayBridge.loadAssetCatalog = loadAssetCatalog
OverlayBridge.patchEffect = bridgePatchEffect
OverlayBridge.previewEffect = bridgePreviewEffect

GLOBAL.OverlayBridge = OverlayBridge

local function runSession()
	disconnectWsConnections()
	Client.sessionId += 1
	local sessionId = Client.sessionId
	Client.avatarReadyRoomId = nil

	local ws = wsConnect(CONFIG.url)
	if ws == nil then
		return false, "no usable WebSocket API (checked WebSocket/websocket/syn.websocket connect variants)"
	end

	Client.ws = ws
	Client.connected = true
	Client.lastServerContact = os.clock()
	emitBridgeState("connected")

	trackWsConnection(ws.OnMessage:Connect(function(raw)
		if Client.sessionId ~= sessionId or Client.ws ~= ws then
			return
		end
		handleServerMessage(raw)
	end))
	trackWsConnection(ws.OnClose:Connect(function()
		if Client.sessionId ~= sessionId or Client.ws ~= ws then
			return
		end
		Client.connected = false
	end))

	local helloResponse, helloError = awaitRequest("hello", {
		client = CONFIG.client_name,
		build = CONFIG.build,
	})
	if helloResponse == nil then
		return false, "hello failed: " .. tostring(helloError)
	end
	Client.heartbeatSeconds = math.max((helloResponse.data.heartbeat_ms or 15000) / 1000, 5)

	local token = CONFIG.token or ("dev:" .. LocalPlayer.Name)
	local authResponse, authError = awaitRequest("auth", { token = token })
	if authResponse == nil then
		return false, "auth failed: " .. tostring(authError)
	end
	Client.user = authResponse.data
	Client.ownAvatarId = "avatar_" .. Client.user.user_id
	log("authenticated as", Client.user.user_id)
	emitBridgeState("authenticated as " .. tostring(Client.user.user_id))
	if CONFIG.asset_catalog_url then
		loadAssetCatalog(CONFIG.asset_catalog_url)
	end

	local roomId = nil
	local resumeFrom = 0
	local roomError = nil
	local pendingRoomId = pendingRoomFromTeleportSetting()
	if pendingRoomId then
		roomId = pendingRoomId
	elseif Client.room and Client.room.id then
		roomId = Client.room.id
		resumeFrom = Client.room.version or 0
	elseif CONFIG.auto_join_default_room then
		roomId, resumeFrom, roomError = resolveRoom()
	end

	if roomId then
		Client.roomClosingId = nil

		local joinResponse, joinError = requestRoomJoin(roomId, resumeFrom or 0)
		if joinResponse == nil and string.find(tostring(joinError), "room.not_found", 1, true) then
			log("previous room missing")
			Client.room = nil
			Client.avatarReadyRoomId = nil
			if CONFIG.auto_join_default_room then
				roomId, resumeFrom, roomError = resolveRoom()
				if roomId == nil then
					return false, roomError
				end
				joinResponse, joinError = requestRoomJoin(roomId, resumeFrom or 0)
			else
				bridgeListRooms()
				emitBridgeState("ready")
				joinResponse = nil
				joinError = nil
			end
		end
		if joinResponse == nil then
			if joinError then
				return false, "room.join failed: " .. tostring(joinError)
			end
		else
			if Client.teleporting then
				return true
			end
			log("joined room", Client.room.id, "at version", joinResponse.data.current_version)

			-- The avatar entity must exist before state.move is accepted.
			local _, avatarError = ensureOwnAvatarEntity(Client.room.id)
			if avatarError then
				return false, avatarError
			end
			emitBridgeState("joined " .. tostring(Client.room.id))
		end
	else
		if roomError then
			return false, roomError
		end
		bridgeListRooms()
		emitBridgeState("ready")
	end

	-- Heartbeat ping loop.
	task.spawn(function()
		while Client.running and Client.connected do
			task.wait(Client.heartbeatSeconds * 0.8)
			sendMessage("ping", { client_time_ms = math.floor(os.clock() * 1000) })
			if os.clock() - Client.lastServerContact > Client.heartbeatSeconds * 3 then
				log("no server contact; forcing reconnect")
				Client.connected = false
				pcall(function()
					Client.ws:Close()
				end)
			end
		end
	end)

	-- Movement loop: local character CFrame -> state.move.
	local moveInterval = 1 / CONFIG.move_hz
	local moveKeepaliveSeconds = CONFIG.move_keepalive_seconds or 10
	local lastMoveCFrame = nil
	local lastMoveRoomId = nil
	local lastMoveEntityId = nil
	local lastMoveSentAt = 0
	while Client.running and Client.connected do
		task.wait(moveInterval)
		local character = LocalPlayer.Character
		local root = character and character:FindFirstChild("HumanoidRootPart")
		if root and Client.room and Client.room.id and Client.roomClosingId ~= Client.room.id and Client.ownAvatarId and Client.avatarReadyRoomId == Client.room.id then
			if lastMoveRoomId ~= Client.room.id or lastMoveEntityId ~= Client.ownAvatarId then
				lastMoveCFrame = nil
				lastMoveRoomId = Client.room.id
				lastMoveEntityId = Client.ownAvatarId
			end
			local cf = root.CFrame
			if not sameCFrameForMove(lastMoveCFrame, cf) or os.clock() - lastMoveSentAt >= moveKeepaliveSeconds then
				lastMoveCFrame = cf
				lastMoveSentAt = os.clock()
				local rx, ry, rz = cf:ToOrientation()
				sendMessage("state.move", {
					room_id = Client.room.id,
					entity_id = Client.ownAvatarId,
					transform = {
						position = { cf.Position.X, cf.Position.Y, cf.Position.Z },
						rotation = { math.deg(rx), math.deg(ry), math.deg(rz) },
					},
				})
			end
		end
	end

	return true
end

-- ---------------------------------------------------------------------------
-- Reconnect loop + shutdown hook
-- ---------------------------------------------------------------------------

GLOBAL.OverlayStop = function()
	Client.running = false
	Client.connected = false
	pcall(function()
		Client.ws:Close()
	end)
	disconnectWsConnections()
	disconnectTrackedConnections()
	clearLocalPreview()
	clearEntities()
	entityFolder:Destroy()
	if GLOBAL.OverlayBridge == OverlayBridge then
		GLOBAL.OverlayBridge = nil
	end
	log("stopped")
end

task.spawn(function()
	local backoff = 1
	while Client.running do
		log("connecting to", CONFIG.url)
		local ok, err = runSession()

		Client.connected = false
		Client.pending = {}
		pcall(function()
			Client.ws:Close()
		end)
		disconnectWsConnections()

		if Client.teleporting then
			log("waiting for Roblox teleport")
			break
		end

		if not Client.running then
			break
		end

		if ok then
			backoff = 1
			log("disconnected; reconnecting")
		else
			log("session error:", err)
			backoff = math.min(backoff * 2, CONFIG.reconnect_max_delay)
		end

		task.wait(backoff)
	end
end)

log("overlay runtime client started; call OverlayStop() to shut down")
]==========]

local EMBEDDED_BRIDGE_UI_LOADER = [==========[-- Obsidian overlay UI loader for the real runtime bridge.
-- Run overlay-runtime/client.lua first. This file does not open a WebSocket;
-- it talks to getgenv().OverlayBridge so UI and runtime share one session.

local OBSIDIAN_URL = "https://raw.githubusercontent.com/deividcomsono/Obsidian/main/Library.lua"

local EMBEDDED_OVERLAY_UI = [=====[-- Obsidian overlay UI prototype for a standalone overlay runtime.
-- This module expects an Obsidian Library instance to be provided by the host.
-- It intentionally contains no executor, injection, bypass, or WebSocket client code.

local OverlayUI = {}
OverlayUI.__index = OverlayUI

local PLACEHOLDER_ROOM = "No rooms"
local PLACEHOLDER_MEMBER = "No members"
local PLACEHOLDER_ASSET = "No catalog assets"

local DEFAULT_THEME = {
	BackgroundColor = Color3.fromRGB(15, 15, 15),
	MainColor = Color3.fromRGB(25, 25, 25),
	AccentColor = Color3.fromRGB(125, 85, 255),
	OutlineColor = Color3.fromRGB(40, 40, 40),
	FontColor = Color3.new(1, 1, 1),
	Font = Font.fromEnum(Enum.Font.Code),
	RedColor = Color3.fromRGB(255, 50, 50),
	DestructiveColor = Color3.fromRGB(220, 38, 38),
	DarkColor = Color3.new(0, 0, 0),
	WhiteColor = Color3.new(1, 1, 1),
}

local DEFAULT_HOOKS = {
	["room.list"] = function() end,
	["room.create"] = function() end,
	["room.join"] = function() end,
	["room.delete"] = function() end,
	["room.invite"] = function() end,
	["room.kick"] = function() end,
	["chat.send"] = function() end,
	["cmd.avatar.set"] = function() end,
	["cmd.morph.apply"] = function() end,
	["cmd.entity.patch"] = function() end,
	["cmd.animation.play"] = function() end,
	["effect.preview"] = function() end,
	OverlayStop = function() end,
}

local function shallowMerge(base, overrides)
	local merged = {}
	for key, value in pairs(base) do
		merged[key] = value
	end
	if overrides then
		for key, value in pairs(overrides) do
			merged[key] = value
		end
	end
	return merged
end

local function roomName(room)
	if typeof(room) == "table" then
		return tostring(room.name or room.title or room.room_id or room.id or room.roomId or "Unnamed room")
	end
	return tostring(room)
end

local function roomId(room)
	if typeof(room) == "table" then
		return room.room_id or room.id or room.roomId
	end
	return room
end

local function memberId(member)
	if typeof(member) == "table" then
		return member.user_id or member.userId or member.id
	end
	return member
end

local function memberName(member)
	if typeof(member) == "table" then
		return tostring(member.display_name or member.displayName or member.name or memberId(member) or "unknown")
	end
	return tostring(member)
end

local function trimString(value)
	if value == nil then
		return ""
	end
	return tostring(value):match("^%s*(.-)%s*$")
end

local function trimHistory(history, limit)
	while #history > limit do
		table.remove(history, 1)
	end
end

function OverlayUI.new(options)
	assert(options and options.Library, "OverlayUI.new requires an Obsidian Library instance")

	local self = setmetatable({}, OverlayUI)
	self.Library = options.Library
	self.Bridge = options.Bridge
	self.Hooks = shallowMerge(DEFAULT_HOOKS, options.Hooks)
	self.Title = options.Title or "Overlay"
	self.Footer = options.Footer or "standalone runtime"
	self.AutoShow = options.AutoShow ~= false
	self.Window = nil
	self.Tabs = {}
	self.Controls = {}
	self.InputValues = {}
	self.RoomByLabel = {}
	self.RoomById = {}
	self.MemberById = {}
	self.AssetById = {}
	self.RefreshingRooms = false
	self.RefreshingMembers = false
	self.RefreshingAssets = false
	self.State = {
		Rooms = {},
		Members = {},
		Assets = {},
		SelectedRoom = nil,
		SelectedMember = nil,
		SelectedAsset = nil,
		JoinedRoom = nil,
		GlobalHistory = {},
		RoomHistory = {},
		RuntimeState = "disconnected",
		LastEvent = "none",
	}

	local theme = shallowMerge(DEFAULT_THEME, options.Theme)
	for key, value in pairs(theme) do
		self.Library.Scheme[key] = value
	end

	if self.Library.SetDPIScale then
		self.Library:SetDPIScale(92)
	end

	return self
end

function OverlayUI:_emit(eventName, payload)
	self.State.LastEvent = eventName
	self:_refreshDiagnostics()

	local hook = self.Hooks[eventName]
	if hook then
		hook(payload)
	end

	local bridge = self.Bridge
	if typeof(bridge) == "function" then
		bridge(eventName, payload)
	elseif typeof(bridge) == "table" then
		local sender = bridge.send or bridge.emit or bridge.Send or bridge.Emit
		if sender then
			sender(eventName, payload)
		end
	end
end

function OverlayUI:_notify(text, seconds)
	if self.Library.Notify then
		self.Library:Notify(text, seconds or 3)
	end
end

function OverlayUI:_setLabel(key, text)
	local label = self.Controls[key]
	if label and label.SetText then
		label:SetText(text)
	end
end

function OverlayUI:_selectedRoomId()
	local selected = self.State.SelectedRoom
	if typeof(selected) == "table" then
		return roomId(selected)
	end
	if type(selected) == "string" and self.RoomById[selected] then
		return selected
	end
	if self.RoomByLabel[selected] then
		return roomId(self.RoomByLabel[selected])
	end
	if type(selected) == "string" and string.sub(selected, 1, 5) == "room_" then
		return selected
	end
	return nil
end

function OverlayUI:_roomLabel(room)
	if typeof(room) ~= "table" then
		return tostring(room)
	end

	local id = tostring(roomId(room) or "")
	local shortId = id ~= "" and string.sub(id, 1, 16) or "no-id"
	local memberCount = room.member_count or 0
	local maxMembers = room.max_members or "?"
	local route = room.roblox_route
	local placeId = route and route.place_id
	local jobId = route and route.job_id
	local routeText = ""
	if placeId then
		local shortJob = type(jobId) == "string" and jobId ~= "" and string.sub(jobId, 1, 8) or "any"
		routeText = string.format(" @%s/%s", tostring(placeId), shortJob)
	end
	return string.format("%s (%s/%s)%s [%s]", roomName(room), tostring(memberCount), tostring(maxMembers), routeText, shortId)
end

function OverlayUI:_roomLabels()
	local labels = {}
	self.RoomByLabel = {}
	self.RoomById = {}
	for _, room in ipairs(self.State.Rooms) do
		local label = self:_roomLabel(room)
		self.RoomByLabel[label] = room
		local id = roomId(room)
		if id then
			self.RoomById[id] = room
		end
		table.insert(labels, label)
	end
	if #labels == 0 then
		table.insert(labels, PLACEHOLDER_ROOM)
	end
	return labels
end

function OverlayUI:_roomValues()
	local values = {}
	self.RoomByLabel = {}
	self.RoomById = {}
	for _, room in ipairs(self.State.Rooms) do
		local id = roomId(room)
		if id then
			self.RoomById[id] = room
			self.RoomByLabel[self:_roomLabel(room)] = room
			table.insert(values, id)
		end
	end
	if #values == 0 then
		table.insert(values, PLACEHOLDER_ROOM)
	end
	return values
end

function OverlayUI:_memberLabel(member)
	if typeof(member) ~= "table" then
		return tostring(member)
	end

	local id = tostring(memberId(member) or "")
	local shortId = id ~= "" and string.sub(id, 1, 22) or "unknown"
	return string.format("%s [%s]", memberName(member), shortId)
end

function OverlayUI:_memberValues()
	local values = {}
	self.MemberById = {}
	for _, member in ipairs(self.State.Members) do
		local id = memberId(member)
		if id then
			self.MemberById[id] = member
			table.insert(values, id)
		end
	end
	if #values == 0 then
		table.insert(values, PLACEHOLDER_MEMBER)
	end
	return values
end

function OverlayUI:_assetValues()
	local values = {}
	self.AssetById = {}
	for _, asset in ipairs(self.State.Assets) do
		if typeof(asset) == "table" and type(asset.asset_id) == "string" then
			self.AssetById[asset.asset_id] = asset
			table.insert(values, asset.asset_id)
		end
	end
	if #values == 0 then
		table.insert(values, PLACEHOLDER_ASSET)
	end
	return values
end

function OverlayUI:_selectedAssetId()
	local selected = self.State.SelectedAsset
	if type(selected) == "string" and self.AssetById[selected] then
		return selected
	end
	if type(selected) == "string" and selected ~= PLACEHOLDER_ASSET then
		return selected
	end
	return nil
end

function OverlayUI:_selectedMemberId()
	local selected = self.State.SelectedMember
	if type(selected) == "string" and self.MemberById[selected] then
		return selected
	end
	if type(selected) == "string" and selected ~= PLACEHOLDER_MEMBER then
		return selected
	end
	return nil
end

function OverlayUI:_upsertMember(member)
	if typeof(member) ~= "table" then
		return
	end

	local id = memberId(member)
	if not id then
		return
	end

	local normalized = {
		user_id = tostring(id),
		display_name = memberName(member),
		joined_at_ms = member.joined_at_ms or member.joinedAt,
	}

	for index, existing in ipairs(self.State.Members) do
		if memberId(existing) == normalized.user_id then
			self.State.Members[index] = normalized
			return
		end
	end
	table.insert(self.State.Members, normalized)
end

function OverlayUI:_removeMember(userId)
	userId = tostring(userId or "")
	if userId == "" then
		return
	end

	local kept = {}
	for _, member in ipairs(self.State.Members) do
		if tostring(memberId(member) or "") ~= userId then
			table.insert(kept, member)
		end
	end
	self.State.Members = kept
	if self.State.SelectedMember == userId then
		self.State.SelectedMember = nil
	end
end

function OverlayUI:_setMembers(members)
	local selectedMemberId = self:_selectedMemberId()
	self.State.Members = {}
	for _, member in ipairs(members or {}) do
		self:_upsertMember(member)
	end
	self.State.SelectedMember = nil
	for _, member in ipairs(self.State.Members) do
		if memberId(member) == selectedMemberId then
			self.State.SelectedMember = selectedMemberId
			break
		end
	end
	self:_refreshMemberControls()
end

function OverlayUI:_findRoomByDisplayName(displayName)
	if self.RoomByLabel[displayName] then
		return self.RoomByLabel[displayName]
	end
	for _, room in ipairs(self.State.Rooms) do
		if self:_roomLabel(room) == displayName or roomName(room) == displayName then
			return room
		end
	end
	return displayName
end

local function auraPresetColor(preset)
	local key = string.lower(tostring(preset or "Violet"))
	local colors = {
		violet = { 155 / 255, 105 / 255, 1 },
		cyan = { 80 / 255, 220 / 255, 1 },
		emerald = { 80 / 255, 1, 170 / 255 },
		gold = { 1, 205 / 255, 80 / 255 },
		rose = { 1, 95 / 255, 150 / 255 },
		none = { 170 / 255, 170 / 255, 170 / 255 },
	}
	return colors[key] or colors.violet
end

function OverlayUI:_rememberInput(key, value)
	self.InputValues[key] = tostring(value or "")
end

function OverlayUI:_readInput(optionKey, cacheKey)
	local options = self.Library.Options or {}
	local option = options[optionKey]
	if option and option.Value ~= nil then
		return trimString(option.Value)
	end
	return trimString(self.InputValues[cacheKey])
end

function OverlayUI:_clearInput(optionKey, cacheKey)
	self.InputValues[cacheKey] = ""
	local options = self.Library.Options or {}
	local option = options[optionKey]
	if option and option.SetValue then
		option:SetValue("")
	end
end

function OverlayUI:_currentEffectPayload()
	local presetOption = self.Library.Options.Overlay_AuraPreset
	local trailOption = self.Library.Options.Overlay_EffectTrail
	local intensityOption = self.Library.Options.Overlay_EffectIntensity
	local preset = presetOption and presetOption.Value or "Violet"
	local intensity = intensityOption and intensityOption.Value or 50
	return {
		preset = string.lower(tostring(preset)),
		intensity = intensity,
		color = auraPresetColor(preset),
		trail = (trailOption and trailOption.Value or "Off") == "On",
	}
end

function OverlayUI:_refreshRoomDropdown()
	local dropdown = self.Controls.RoomDropdown
	if not dropdown then
		return
	end

	local values = self:_roomValues()
	local selectedRoomId = self:_selectedRoomId()
	local selectedValue = selectedRoomId and self.RoomById[selectedRoomId] and selectedRoomId or nil
	if selectedValue == nil and #self.State.Rooms == 0 then
		selectedValue = PLACEHOLDER_ROOM
	end

	self.RefreshingRooms = true
	if dropdown.SetValues then
		dropdown:SetValues(values)
	end
	if dropdown.SetDisabledValues then
		dropdown:SetDisabledValues(#self.State.Rooms == 0 and { PLACEHOLDER_ROOM } or {})
	end
	if dropdown.SetValue then
		dropdown:SetValue(selectedValue)
	end
	self.RefreshingRooms = false
end

function OverlayUI:_refreshChatLabels()
	self:_setLabel("GlobalHistoryLabel", table.concat(self.State.GlobalHistory, "\n"))
	self:_setLabel("RoomHistoryLabel", table.concat(self.State.RoomHistory, "\n"))
	self:_setLabel("JoinedRoomLabel", "Joined: " .. tostring(self.State.JoinedRoom or "none"))
end

function OverlayUI:_refreshMemberControls()
	local dropdown = self.Controls.MemberDropdown
	local values = self:_memberValues()
	local selectedMemberId = self:_selectedMemberId()
	local selectedValue = selectedMemberId and self.MemberById[selectedMemberId] and selectedMemberId or nil
	if selectedValue == nil and #self.State.Members == 0 then
		selectedValue = PLACEHOLDER_MEMBER
	end

	if dropdown then
		self.RefreshingMembers = true
		if dropdown.SetValues then
			dropdown:SetValues(values)
		end
		if dropdown.SetDisabledValues then
			dropdown:SetDisabledValues(#self.State.Members == 0 and { PLACEHOLDER_MEMBER } or {})
		end
		if dropdown.SetValue then
			dropdown:SetValue(selectedValue)
		end
		self.RefreshingMembers = false
	end

	local lines = {}
	for _, member in ipairs(self.State.Members) do
		table.insert(lines, self:_memberLabel(member))
	end
	if #lines == 0 then
		table.insert(lines, "No joined members yet.")
	end
	self:_setLabel("MembersLabel", table.concat(lines, "\n"))
end

function OverlayUI:_refreshAssetDropdown()
	local dropdown = self.Controls.MorphAssetDropdown
	if not dropdown then
		return
	end

	local values = self:_assetValues()
	local selectedAssetId = self:_selectedAssetId()
	local selectedValue = selectedAssetId and self.AssetById[selectedAssetId] and selectedAssetId or nil
	if selectedValue == nil and #self.State.Assets == 0 then
		selectedValue = PLACEHOLDER_ASSET
	end

	self.RefreshingAssets = true
	if dropdown.SetValues then
		dropdown:SetValues(values)
	end
	if dropdown.SetDisabledValues then
		dropdown:SetDisabledValues(#self.State.Assets == 0 and { PLACEHOLDER_ASSET } or {})
	end
	if dropdown.SetValue then
		dropdown:SetValue(selectedValue)
	end
	self.RefreshingAssets = false
end

function OverlayUI:_refreshDiagnostics()
	self:_setLabel("RuntimeStateLabel", "Runtime: " .. tostring(self.State.RuntimeState))
	self:_setLabel("LastEventLabel", "Last event: " .. tostring(self.State.LastEvent))
	self:_setLabel("RoomCountLabel", "Rooms cached: " .. tostring(#self.State.Rooms))
end

function OverlayUI:Mount()
	local Library = self.Library

	self.Window = Library:CreateWindow({
		Title = self.Title,
		Footer = self.Footer,
		Position = UDim2.fromOffset(12, 72),
		Size = UDim2.fromOffset(540, 392),
		Center = false,
		AutoShow = self.AutoShow,
		Resizable = true,
		NotifySide = "Right",
		ShowCustomCursor = false,
		ShowMobileButtons = true,
		MobileButtonsSide = "Right",
		UnlockMouseWhileOpen = true,
		EnableSidebarResize = true,
		EnableCompacting = true,
		SidebarCompacted = true,
		MinContainerWidth = 232,
		MinSidebarWidth = 112,
		SidebarCompactWidth = 48,
		SidebarCollapseThreshold = 0.58,
		CompactWidthActivation = 96,
		SearchbarSize = UDim2.fromScale(0.72, 1),
		ToggleKeybind = Enum.KeyCode.RightShift,
		CornerRadius = 4,
	})

	self.Tabs = {
		Rooms = self.Window:AddTab({ Name = "Rooms", Icon = "door-open", Description = "Create, refresh, and join rooms" }),
		GlobalChat = self.Window:AddTab({ Name = "Global Chat", Icon = "messages-square", Description = "Global overlay channel" }),
		RoomChat = self.Window:AddTab({ Name = "Room Chat", Icon = "message-circle", Description = "Selected room channel" }),
		Avatar = self.Window:AddTab({ Name = "Avatar/Effects", Icon = "sparkles", Description = "Avatar, morph, and animation commands" }),
		Diagnostics = self.Window:AddTab({ Name = "Diagnostics", Icon = "activity", Description = "Runtime status and cleanup" }),
	}

	self:_buildRooms()
	self:_buildGlobalChat()
	self:_buildRoomChat()
	self:_buildAvatarEffects()
	self:_buildDiagnostics()
	self:_refreshDiagnostics()
	self:_refreshChatLabels()
	self:_refreshMemberControls()

	if self.Window.SetCompact then
		self.Window:SetCompact(true)
	end

	return self
end

function OverlayUI:_buildRooms()
	local session = self.Tabs.Rooms:AddLeftGroupbox("Session", "door-open")
	local members = self.Tabs.Rooms:AddLeftGroupbox("Members", "users")
	local actions = self.Tabs.Rooms:AddRightGroupbox("Room Actions", "list-plus")

	self.Controls.RoomCountLabel = session:AddLabel("Overlay_RoomCount", {
		Text = "Rooms cached: 0",
		DoesWrap = false,
	})
	session:AddButton({
		Text = "Refresh rooms",
		Tooltip = "Requests room.list from the overlay runtime.",
		Func = function()
			self:_emit("room.list", {})
			self:_notify("Requested room list", 2)
		end,
	})

	self.Controls.RoomDropdown = session:AddDropdown("Overlay_RoomList", {
		Text = "Known rooms",
		Values = { PLACEHOLDER_ROOM },
		Default = 1,
		AllowNull = true,
		Searchable = true,
		MaxVisibleDropdownItems = 6,
		DisabledValues = { PLACEHOLDER_ROOM },
		FormatDisplayValue = function(value)
			if value == PLACEHOLDER_ROOM then
				return PLACEHOLDER_ROOM
			end
			local room = self.RoomById[value]
			if room then
				return self:_roomLabel(room)
			end
			return tostring(value or "")
		end,
		Callback = function(value)
			if self.RefreshingRooms then
				return
			end
			if value == PLACEHOLDER_ROOM then
				self.State.SelectedRoom = nil
				return
			end
			if type(value) == "string" and self.RoomById[value] then
				self.State.SelectedRoom = value
			else
				self.State.SelectedRoom = nil
			end
		end,
	})

	actions:AddInput("Overlay_CreateRoomName", {
		Text = "Room name",
		Placeholder = "Studio A",
		Default = "",
		Finished = false,
		ClearTextOnFocus = false,
		AllowEmpty = true,
		Callback = function(value)
			self:_rememberInput("CreateRoomName", value)
		end,
	})
	actions:AddButton({
		Text = "Create room",
		Tooltip = "Emits room.create with the typed room name.",
		Func = function()
			local name = self:_readInput("Overlay_CreateRoomName", "CreateRoomName")
			if name == "" then
				self:_notify("Room name is required", 2)
				return
			end
			self:_emit("room.create", { name = name })
		end,
	})
	actions:AddButton({
		Text = "Join selected",
		Tooltip = "Emits room.join with the selected room id.",
		Func = function()
			local roomId = self:_selectedRoomId()
			if not roomId or roomId == PLACEHOLDER_ROOM then
				self:_notify("Select a room first", 2)
				return
			end
			self:_emit("room.join", { room_id = roomId })
		end,
	})
	actions:AddButton({
		Text = "Close room",
		Risky = true,
		DoubleClick = true,
		Tooltip = "Double click to close the room you are currently in.",
		Func = function()
			local roomId = self.State.JoinedRoom
			if not roomId or roomId == PLACEHOLDER_ROOM then
				self:_notify("Join a room before closing it", 2)
				return
			end
			self:_emit("room.delete", { room_id = roomId })
		end,
	})
	actions:AddInput("Overlay_InviteUserId", {
		Text = "Invite user id",
		Placeholder = "dev_PlayerName",
		Default = "",
		Finished = false,
		ClearTextOnFocus = false,
		AllowEmpty = true,
		Callback = function(value)
			self:_rememberInput("InviteUserId", value)
		end,
	})
	actions:AddButton({
		Text = "Invite user",
		Tooltip = "Invites a user id to the joined private room.",
		Func = function()
			local userId = self:_readInput("Overlay_InviteUserId", "InviteUserId")
			if userId == "" then
				self:_notify("User id is required", 2)
				return
			end
			if not self.State.JoinedRoom then
				self:_notify("Join a room before inviting", 2)
				return
			end
			self:_emit("room.invite", {
				room_id = self.State.JoinedRoom,
				user_id = userId,
			})
		end,
	})

	self.Controls.MembersLabel = members:AddLabel("Overlay_Members", {
		Text = "No joined members yet.",
		DoesWrap = true,
		Size = 13,
	})
	self.Controls.MemberDropdown = members:AddDropdown("Overlay_MemberList", {
		Text = "Room members",
		Values = { PLACEHOLDER_MEMBER },
		Default = 1,
		AllowNull = true,
		Searchable = true,
		MaxVisibleDropdownItems = 6,
		DisabledValues = { PLACEHOLDER_MEMBER },
		FormatDisplayValue = function(value)
			if value == PLACEHOLDER_MEMBER then
				return PLACEHOLDER_MEMBER
			end
			local member = self.MemberById[value]
			if member then
				return self:_memberLabel(member)
			end
			return tostring(value or "")
		end,
		Callback = function(value)
			if self.RefreshingMembers then
				return
			end
			if value == PLACEHOLDER_MEMBER then
				self.State.SelectedMember = nil
				return
			end
			if type(value) == "string" and self.MemberById[value] then
				self.State.SelectedMember = value
			else
				self.State.SelectedMember = nil
			end
		end,
	})
	members:AddButton({
		Text = "Kick selected",
		Risky = true,
		DoubleClick = true,
		Tooltip = "Double click to remove the selected user from the joined room.",
		Func = function()
			local userId = self:_selectedMemberId()
			if not self.State.JoinedRoom then
				self:_notify("Join a room before kicking", 2)
				return
			end
			if not userId or userId == PLACEHOLDER_MEMBER then
				self:_notify("Select a member first", 2)
				return
			end
			self:_emit("room.kick", {
				room_id = self.State.JoinedRoom,
				user_id = userId,
			})
		end,
	})
end

function OverlayUI:_buildGlobalChat()
	local compose = self.Tabs.GlobalChat:AddLeftGroupbox("Compose", "send")
	local history = self.Tabs.GlobalChat:AddRightGroupbox("Recent", "message-square-text")

	compose:AddInput("Overlay_GlobalMessage", {
		Text = "Message",
		Placeholder = "Send to everyone",
		Default = "",
		Finished = false,
		ClearTextOnFocus = false,
		MaxLength = 160,
		Callback = function(value)
			self:_rememberInput("GlobalMessage", value)
		end,
	})
	compose:AddButton({
		Text = "Send global",
		Func = function()
			local text = self:_readInput("Overlay_GlobalMessage", "GlobalMessage")
			if text == "" then
				self:_notify("Message is empty", 2)
				return
			end
			self:_emit("chat.send", { scope = "global", text = text })
			self:_clearInput("Overlay_GlobalMessage", "GlobalMessage")
		end,
	})

	self.Controls.GlobalHistoryLabel = history:AddLabel("Overlay_GlobalHistory", {
		Text = "No global messages yet.",
		DoesWrap = true,
		Size = 13,
	})
end

function OverlayUI:_buildRoomChat()
	local compose = self.Tabs.RoomChat:AddLeftGroupbox("Compose", "send-horizontal")
	local history = self.Tabs.RoomChat:AddRightGroupbox("Room Recent", "message-circle")

	self.Controls.JoinedRoomLabel = compose:AddLabel("Overlay_JoinedRoom", {
		Text = "Joined: none",
		DoesWrap = false,
	})
	compose:AddInput("Overlay_RoomMessage", {
		Text = "Room message",
		Placeholder = "Send to joined room",
		Default = "",
		Finished = false,
		ClearTextOnFocus = false,
		MaxLength = 160,
		Callback = function(value)
			self:_rememberInput("RoomMessage", value)
		end,
	})
	compose:AddButton({
		Text = "Send room",
		Func = function()
			local text = self:_readInput("Overlay_RoomMessage", "RoomMessage")
			if text == "" then
				self:_notify("Message is empty", 2)
				return
			end
			self:_emit("chat.send", {
				scope = "room",
				room_id = self.State.JoinedRoom or self:_selectedRoomId(),
				text = text,
			})
			self:_clearInput("Overlay_RoomMessage", "RoomMessage")
		end,
	})

	self.Controls.RoomHistoryLabel = history:AddLabel("Overlay_RoomHistory", {
		Text = "No room messages yet.",
		DoesWrap = true,
		Size = 13,
	})
end

function OverlayUI:_buildAvatarEffects()
	local avatar = self.Tabs.Avatar:AddLeftGroupbox("Avatar", "user-round")
	local effects = self.Tabs.Avatar:AddRightGroupbox("Effects", "sparkles")

	avatar:AddInput("Overlay_AvatarId", {
		Text = "Avatar asset / user id",
		Placeholder = "asset id, GitHub raw URL, or user:123",
		Default = "",
		Finished = true,
		ClearTextOnFocus = false,
	})
	avatar:AddButton({
		Text = "Set avatar",
		Tooltip = "Emits cmd.avatar.set.",
		Func = function()
			local option = self.Library.Options.Overlay_AvatarId
			self:_emit("cmd.avatar.set", { value = option and option.Value or "" })
		end,
	})

	effects:AddInput("Overlay_MorphAssetId", {
		Text = "Morph asset id / URL",
		Placeholder = "registered id or GitHub raw URL",
		Default = "",
		Finished = true,
		ClearTextOnFocus = false,
	})
	self.Controls.MorphAssetDropdown = effects:AddDropdown("Overlay_MorphCatalog", {
		Text = "Catalog morph",
		Values = { PLACEHOLDER_ASSET },
		Default = 1,
		Searchable = true,
		MaxVisibleDropdownItems = 8,
		DisabledValues = { PLACEHOLDER_ASSET },
		Callback = function(value)
			if self.RefreshingAssets then
				return
			end
			self.State.SelectedAsset = value
		end,
	})
	effects:AddDropdown("Overlay_MorphPreset", {
		Text = "Morph preset",
		Values = { "None", "Robot", "Ghost", "Custom" },
		Default = 1,
		Searchable = true,
	})
	effects:AddButton({
		Text = "Apply morph",
		Tooltip = "Emits cmd.morph.apply.",
		Func = function()
			local asset = self.Library.Options.Overlay_MorphAssetId
			local option = self.Library.Options.Overlay_MorphPreset
			local value = trimString(asset and asset.Value or "")
			if value ~= "" then
				self:_emit("cmd.morph.apply", { value = value })
			elseif self:_selectedAssetId() then
				self:_emit("cmd.morph.apply", { asset_id = self:_selectedAssetId() })
			else
				self:_emit("cmd.morph.apply", { preset = option and option.Value or "None" })
			end
		end,
	})
	effects:AddDropdown("Overlay_AuraPreset", {
		Text = "Aura preset",
		Values = { "Violet", "Cyan", "Emerald", "Gold", "Rose", "None" },
		Default = 1,
		Searchable = true,
	})
	effects:AddDropdown("Overlay_AnimationPreset", {
		Text = "Animation",
		Values = { "Wave", "Dance", "Idle", "Point" },
		Default = 1,
		Searchable = true,
	})
	effects:AddInput("Overlay_AnimationAssetId", {
		Text = "Animation asset id / URL",
		Placeholder = "optional",
		Default = "",
		Finished = true,
		ClearTextOnFocus = false,
	})
	effects:AddDropdown("Overlay_EffectTrail", {
		Text = "Trail",
		Values = { "Off", "On" },
		Default = 1,
		Searchable = false,
	})
	effects:AddSlider("Overlay_EffectIntensity", {
		Text = "Effect intensity",
		Default = 50,
		Min = 0,
		Max = 100,
		Rounding = 0,
		Suffix = "%",
		Compact = true,
	})
	effects:AddButton({
		Text = "Apply effect",
		Tooltip = "Applies a client-rendered aura effect to your room avatar.",
		Func = function()
			self:_emit("cmd.entity.patch", {
				effect = self:_currentEffectPayload(),
			})
		end,
	})
	effects:AddButton({
		Text = "Preview local",
		Tooltip = "Shows the selected effect on your own character without syncing it to the room.",
		Func = function()
			self:_emit("effect.preview", {
				effect = self:_currentEffectPayload(),
			})
		end,
	})
	effects:AddButton({
		Text = "Play animation",
		Tooltip = "Emits cmd.animation.play.",
		Func = function()
			local animation = self.Library.Options.Overlay_AnimationPreset
			local intensity = self.Library.Options.Overlay_EffectIntensity
			local asset = self.Library.Options.Overlay_AnimationAssetId
			local value = trimString(asset and asset.Value or "")
			self:_emit("cmd.animation.play", {
				name = animation and animation.Value or "Wave",
				intensity = intensity and intensity.Value or 50,
				value = value,
			})
		end,
	})
end

function OverlayUI:_buildDiagnostics()
	local status = self.Tabs.Diagnostics:AddLeftGroupbox("Status", "activity")
	local lifecycle = self.Tabs.Diagnostics:AddRightGroupbox("Lifecycle", "power")

	self.Controls.RuntimeStateLabel = status:AddLabel("Overlay_RuntimeState", {
		Text = "Runtime: disconnected",
		DoesWrap = false,
	})
	self.Controls.LastEventLabel = status:AddLabel("Overlay_LastEvent", {
		Text = "Last event: none",
		DoesWrap = false,
	})
	status:AddButton({
		Text = "Ping runtime",
		Func = function()
			self:_emit("diagnostics.ping", {})
			self:_notify("Ping emitted", 2)
		end,
	})

	lifecycle:AddLabel({
		Text = "OverlayStop is exposed as a bridge hook so the host runtime can close sockets, detach handlers, and unload this UI in its own order.",
		DoesWrap = true,
		Size = 13,
	})
	lifecycle:AddButton({
		Text = "Stop overlay",
		Risky = true,
		DoubleClick = true,
		Tooltip = "Double click to emit OverlayStop.",
		Func = function()
			self:_emit("OverlayStop", {})
			if self.Library.Unload then
				self.Library:Unload()
			end
		end,
	})
end

function OverlayUI:SetRuntimeState(state)
	self.State.RuntimeState = state
	self:_refreshDiagnostics()
end

function OverlayUI:SetRooms(rooms)
	local selectedRoomId = self:_selectedRoomId()
	self.State.Rooms = rooms or {}
	self.State.SelectedRoom = nil
	if selectedRoomId then
		for _, room in ipairs(self.State.Rooms) do
			if roomId(room) == selectedRoomId then
				self.State.SelectedRoom = selectedRoomId
				break
			end
		end
	end
	self:_refreshRoomDropdown()
	self:_refreshDiagnostics()
end

function OverlayUI:AppendGlobalMessage(author, text)
	if text == nil or text == "" then
		return
	end
	table.insert(self.State.GlobalHistory, string.format("[%s] %s", author or "?", text))
	trimHistory(self.State.GlobalHistory, 8)
	self:_refreshChatLabels()
end

function OverlayUI:AppendRoomMessage(author, text)
	if text == nil or text == "" then
		return
	end
	table.insert(self.State.RoomHistory, string.format("[%s] %s", author or "?", text))
	trimHistory(self.State.RoomHistory, 8)
	self:_refreshChatLabels()
end

function OverlayUI:HandleRuntimeEvent(eventName, payload)
	self.State.LastEvent = eventName

	if eventName == "room.list" then
		local rooms = payload and (payload.rooms or (payload.data and payload.data.rooms) or payload.data) or {}
		self:SetRooms(rooms)
	elseif eventName == "room.join" then
		local joinedRoomId = payload and (payload.room_id or payload.id or payload.roomId)
		if joinedRoomId then
			self.State.JoinedRoom = joinedRoomId
			self.State.SelectedRoom = joinedRoomId
			self.State.Members = {}
			self.State.SelectedMember = nil
			self:_refreshRoomDropdown()
			self:_refreshChatLabels()
			self:_refreshMemberControls()
		end
	elseif eventName == "room.snapshot" then
		local snapshotRoomId = payload and (payload.room_id or payload.id or payload.roomId)
		if snapshotRoomId and (not self.State.JoinedRoom or self.State.JoinedRoom == snapshotRoomId) then
			self.State.JoinedRoom = snapshotRoomId
			self.State.SelectedRoom = snapshotRoomId
			self:_setMembers(payload.members or {})
			self:_refreshRoomDropdown()
			self:_refreshChatLabels()
		end
	elseif eventName == "room.member.joined" then
		local memberRoomId = payload and (payload.room_id or payload.id or payload.roomId)
		if memberRoomId and self.State.JoinedRoom == memberRoomId then
			self:_upsertMember(payload)
			self:_refreshMemberControls()
		end
	elseif eventName == "room.member.left" then
		local memberRoomId = payload and (payload.room_id or payload.id or payload.roomId)
		if memberRoomId and self.State.JoinedRoom == memberRoomId then
			self:_removeMember(payload and (payload.user_id or payload.userId))
			self:_refreshMemberControls()
		end
	elseif eventName == "room.deleted" then
		local roomIdToRemove = payload and (payload.room_id or payload.id or payload.roomId)
		if roomIdToRemove then
			local kept = {}
			for _, room in ipairs(self.State.Rooms) do
				if roomId(room) ~= roomIdToRemove then
					table.insert(kept, room)
				end
			end
			self.State.Rooms = kept
			if self.State.JoinedRoom == roomIdToRemove then
				self.State.JoinedRoom = nil
				self.State.Members = {}
				self.State.SelectedMember = nil
			end
			self.State.SelectedRoom = nil
			self:_refreshRoomDropdown()
			self:_refreshChatLabels()
			self:_refreshMemberControls()
		end
	elseif eventName == "room.kicked" then
		local roomIdToClear = payload and (payload.room_id or payload.id or payload.roomId)
		if not roomIdToClear or self.State.JoinedRoom == roomIdToClear then
			self.State.JoinedRoom = nil
			self.State.SelectedRoom = nil
			self.State.Members = {}
			self.State.SelectedMember = nil
			self:_refreshRoomDropdown()
			self:_refreshChatLabels()
			self:_refreshMemberControls()
		end
		self:_notify("Removed from room", 4)
	elseif eventName == "room.invited" then
		local invitedRoomId = payload and (payload.room_id or payload.id or payload.roomId)
		if invitedRoomId then
			local replaced = false
			for index, room in ipairs(self.State.Rooms) do
				if roomId(room) == invitedRoomId then
					self.State.Rooms[index] = payload
					replaced = true
					break
				end
			end
			if not replaced then
				table.insert(self.State.Rooms, payload)
			end
			self.State.SelectedRoom = invitedRoomId
			self:_refreshRoomDropdown()
			self:_notify("Room invite received", 4)
		end
	elseif eventName == "chat.global" then
		self:AppendGlobalMessage(payload and (payload.author or payload.display_name or payload.user) or "remote", payload and payload.text or "")
	elseif eventName == "chat.room" then
		self:AppendRoomMessage(payload and (payload.author or payload.display_name or payload.user) or "remote", payload and payload.text or "")
	elseif eventName == "asset.registered" then
		local asset = payload and payload.asset
		self:_notify("Asset registered: " .. tostring(asset and asset.asset_id or "?"), 3)
	elseif eventName == "asset.catalog" then
		self.State.Assets = payload and payload.assets or {}
		self.State.SelectedAsset = nil
		self:_refreshAssetDropdown()
		self:_notify("Asset catalog loaded: " .. tostring(#self.State.Assets), 3)
	elseif eventName == "asset.cached" then
		self:_notify("Asset cache: " .. tostring(payload and payload.asset_id or "?") .. " / " .. tostring(payload and payload.status or "?"), 3)
	elseif eventName == "asset.descriptor.ready" then
		self:_notify("Descriptor ready: " .. tostring(payload and (payload.name or payload.asset_id) or "?"), 3)
	elseif eventName == "avatar.applied" then
		self:_notify("Avatar applied", 3)
	elseif eventName == "avatar.appearance.ready" then
		self:_notify("Avatar appearance ready", 3)
	elseif eventName == "morph.applied" then
		self:_notify("Morph applied", 3)
	elseif eventName == "animation.played" then
		self:_notify("Animation played", 3)
	elseif eventName == "diagnostics.state" then
		self:SetRuntimeState(payload and payload.state or "connected")
	elseif eventName == "error" then
		self:_notify(payload and (payload.message or payload.code) or "Overlay error", 4)
	end

	self:_refreshDiagnostics()
end

function OverlayUI:Destroy()
	if self.Library and self.Library.Unload then
		self.Library:Unload()
	end
end

return OverlayUI
]=====]

local GLOBAL = typeof(getgenv) == "function" and getgenv() or _G

if type(GLOBAL.OverlayUIBridgeUnload) == "function" then
	pcall(GLOBAL.OverlayUIBridgeUnload)
end

local function fetchText(url)
	assert(type(game.HttpGet) == "function", "game:HttpGet is required for GitHub test loading")
	local ok, result = pcall(function()
		return game:HttpGet(url)
	end)
	if not ok then
		error("Failed to fetch " .. url .. ": " .. tostring(result), 2)
	end
	return result
end

local function loadChunk(name, source)
	local loader = loadstring or load
	assert(type(loader) == "function", "loadstring or load is required")
	local fn, err = loader(source)
	if not fn then
		error("Failed to compile " .. name .. ": " .. tostring(err), 2)
	end
	local ok, result = pcall(fn)
	if not ok then
		error("Failed to run " .. name .. ": " .. tostring(result), 2)
	end
	return result
end

local Bridge = GLOBAL.OverlayBridge
if type(Bridge) ~= "table" then
	error("OverlayBridge not found. Run overlay-runtime/client.lua first, wait for it to connect, then run this UI loader.", 2)
end

local Library = loadChunk("Obsidian Library", fetchText(OBSIDIAN_URL))
local OverlayUI = loadChunk("embedded overlay_ui.lua", EMBEDDED_OVERLAY_UI)

local ui
local unsubscribe

local function bridgeSend(eventName, payload)
	payload = payload or {}
	if type(Bridge.send) == "function" then
		Bridge.send(eventName, payload)
	elseif type(Bridge.emit) == "function" then
		Bridge.emit(eventName, payload)
	else
		if ui then
			ui:HandleRuntimeEvent("error", {
				code = "bridge.missing_send",
				message = "Runtime bridge has no send function",
			})
		end
	end
end

local function onBridgeEvent(eventName, payload)
	if ui then
		ui:HandleRuntimeEvent(eventName, payload or {})
	end
end

if type(Bridge.onEvent) == "function" then
	unsubscribe = Bridge.onEvent(onBridgeEvent)
elseif type(Bridge.OnEvent) == "function" then
	unsubscribe = Bridge.OnEvent(onBridgeEvent)
else
	error("OverlayBridge has no event subscription function", 2)
end

ui = OverlayUI.new({
	Library = Library,
	Title = "Overlay Runtime",
	Footer = "Runtime bridge",
	Bridge = {
		send = bridgeSend,
	},
})

ui:Mount()
ui:SetRuntimeState("bridge attached")

if type(Bridge.getState) == "function" then
	local state = Bridge.getState()
	if type(state) == "table" then
		if state.rooms then
			ui:HandleRuntimeEvent("room.list", { rooms = state.rooms })
		end
		if state.assets then
			ui:HandleRuntimeEvent("asset.catalog", { assets = state.assets })
		end
		if state.room and state.room.id then
			ui:HandleRuntimeEvent("room.join", { room_id = state.room.id })
		end
		if state.connected then
			ui:SetRuntimeState("connected")
		end
	end
end

bridgeSend("room.list", {})

GLOBAL.OverlayUIBridge = ui
GLOBAL.OverlayUIBridgeUnload = function()
	if type(unsubscribe) == "function" then
		pcall(unsubscribe)
	end
	if Library and Library.Unload then
		pcall(function()
			Library:Unload()
		end)
	end
	if GLOBAL.OverlayUIBridge == ui then
		GLOBAL.OverlayUIBridge = nil
	end
end

return ui
]==========]

local GLOBAL = typeof(getgenv) == "function" and getgenv() or _G

local function log(...)
	print("[overlay-combined]", ...)
end

local function loadChunk(name, source)
	local loader = loadstring or load
	assert(type(loader) == "function", "loadstring or load is required")
	local fn, err = loader(source)
	if not fn then
		error("Failed to compile " .. name .. ": " .. tostring(err), 2)
	end
	local ok, result = pcall(fn)
	if not ok then
		error("Failed to run " .. name .. ": " .. tostring(result), 2)
	end
	return result
end

loadChunk("overlay-runtime/client.lua", EMBEDDED_RUNTIME_CLIENT)

local deadline = os.clock() + 12
while os.clock() < deadline do
	local bridge = GLOBAL.OverlayBridge
	if type(bridge) == "table" then
		local connected = false
		if type(bridge.getState) == "function" then
			local ok, state = pcall(bridge.getState)
			connected = ok and type(state) == "table" and state.connected == true
		end
		if connected then
			break
		end
	end
	task.wait(0.1)
end

if type(GLOBAL.OverlayBridge) ~= "table" then
	error("OverlayBridge did not start; runtime client failed before UI mount", 2)
end

log("mounting bridge UI")
return loadChunk("github_bridge_loader_bundled.lua", EMBEDDED_BRIDGE_UI_LOADER)
