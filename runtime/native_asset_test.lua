-- Overlay native asset loading probe.
-- Run this alone in the executor to check whether native .rbxmx/.rbxm files
-- can be downloaded, cached, loaded with game:GetObjects, and parented.

local GLOBAL = typeof(getgenv) == "function" and getgenv() or _G

local TEST_URL = GLOBAL.OverlayNativeAssetTestUrl
	or "https://cdn.jsdelivr.net/gh/mamiisss/overlay@main/native/samples/sample_morph.rbxmx"
local CACHE_FOLDER = "overlay-cache/native-test"
local CACHE_FILE = CACHE_FOLDER .. "/sample_morph.rbxmx"

local function log(...)
	print("[overlay-native-test]", ...)
end

local function getGlobalFunction(name)
	local value = GLOBAL and GLOBAL[name] or nil
	if type(value) == "function" then
		return value
	end
	value = _G and _G[name] or nil
	if type(value) == "function" then
		return value
	end
	return nil
end

local function requestBody(url)
	if game and game.HttpGet then
		local ok, body = pcall(function()
			return game:HttpGet(url)
		end)
		if ok and type(body) == "string" then
			return body
		end
	end

	local requestFn = getGlobalFunction("request")
		or (type(syn) == "table" and syn.request)
		or (type(http) == "table" and http.request)
		or getGlobalFunction("http_request")

	if requestFn then
		local ok, response = pcall(requestFn, { Url = url, Method = "GET" })
		if ok and type(response) == "table" and type(response.Body) == "string" then
			return response.Body
		end
	end

	return nil
end

local function ensureFolder(path)
	local isfolder = getGlobalFunction("isfolder")
	local makefolder = getGlobalFunction("makefolder")
	if not isfolder or not makefolder then
		return false
	end
	local current = ""
	for part in string.gmatch(path, "[^/]+") do
		current = current == "" and part or (current .. "/" .. part)
		if not isfolder(current) then
			local ok = pcall(makefolder, current)
			if not ok and not isfolder(current) then
				return false
			end
		end
	end
	return true
end

local function loadNativeObjects(assetUrl)
	if type(game.GetObjects) ~= "function" then
		return nil, "game.GetObjects missing"
	end
	local ok, objects = pcall(function()
		return game:GetObjects(assetUrl)
	end)
	if not ok then
		return nil, tostring(objects)
	end
	if type(objects) ~= "table" or #objects == 0 then
		return nil, "game:GetObjects returned no objects"
	end
	return objects
end

log("downloading", TEST_URL)

local writefile = getGlobalFunction("writefile")
local readfile = getGlobalFunction("readfile")
local isfile = getGlobalFunction("isfile")
local getcustomasset = getGlobalFunction("getcustomasset")

if not writefile or not readfile or not isfile or not getcustomasset then
	error("filesystem/getcustomasset API missing")
end

if not ensureFolder(CACHE_FOLDER) then
	error("could not create cache folder")
end

local body = requestBody(TEST_URL)
if type(body) ~= "string" or #body == 0 then
	error("download failed")
end

writefile(CACHE_FILE, body)
if not isfile(CACHE_FILE) then
	error("cache write failed")
end

local localAssetUrl = getcustomasset(CACHE_FILE)
log("cached", CACHE_FILE, "bytes", #body, "asset", tostring(localAssetUrl))

local objects, err = loadNativeObjects(localAssetUrl)
if not objects then
	log("local getcustomasset load failed:", err)
	log("trying direct URL load")
	objects, err = loadNativeObjects(TEST_URL)
end
if not objects then
	error("native load failed: " .. tostring(err))
end

local folder = workspace:FindFirstChild("OverlayNativeAssetTest")
if folder then
	folder:Destroy()
end
folder = Instance.new("Folder")
folder.Name = "OverlayNativeAssetTest"
folder.Parent = workspace

for index, object in ipairs(objects) do
	object.Name = "NativeLoaded_" .. tostring(index) .. "_" .. object.Name
	object.Parent = folder
	if object:IsA("Model") then
		pcall(function()
			object:PivotTo(CFrame.new(0, 8, -8))
		end)
	elseif object:IsA("BasePart") then
		object.CFrame = CFrame.new(0, 8, -8)
	end
end

log("loaded native objects:", #objects, "parented to workspace.OverlayNativeAssetTest")
