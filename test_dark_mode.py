from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(record_video_dir="/tmp/video")
        page = context.new_page()

        # Try to login or directly navigate to the target page if possible
        page.goto("http://localhost:3000")
        page.wait_for_timeout(1000)

        # Click login mock button if it exists
        try:
            page.locator('button:has-text("Mock Login")').click(timeout=2000)
            page.wait_for_timeout(1000)
        except:
            pass

        # Navigate to the relevant page
        page.goto("http://localhost:3000/analise-produtos", timeout=5000)
        page.wait_for_timeout(2000)

        # enable dark mode
        page.evaluate("localStorage.setItem('theme', 'dark'); document.documentElement.classList.add('dark');")
        page.wait_for_timeout(1000)

        # go to /revisao-final
        page.goto("http://localhost:3000/revisao-final", timeout=5000)
        page.wait_for_timeout(2000)

        # check if it loaded, maybe take a screenshot
        page.screenshot(path="/tmp/verification.png")
        page.wait_for_timeout(1000)

        context.close()
        browser.close()

if __name__ == '__main__':
    run()
